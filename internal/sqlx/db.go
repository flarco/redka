package sqlx

import (
	"context"
	"database/sql"
	_ "embed"
	"net/url"
	"runtime"
	"strings"
	"sync"
)

// Driver type constants
const (
	DriverSQLite   = "sqlite"
	DriverPostgres = "postgres"
)

// Database schema version.
// const schemaVersion = 1

//go:embed schema.sql
var sqlSchema string

//go:embed schema_postgres.sql
var postgresSchema string

// DefaultPragma is a set of default database settings.
var DefaultPragma = map[string]string{
	"journal_mode": "wal",
	"synchronous":  "normal",
	"temp_store":   "memory",
	"mmap_size":    "268435456",
	"foreign_keys": "on",
}

// DB is a generic database-backed repository
// with a domain-specific transaction of type T.
// Has separate database handles for read-write
// and read-only operations.
type DB[T any] struct {
	RW     *sql.DB    // read-write handle
	RO     *sql.DB    // read-only handle
	Driver string     // database driver type
	newT   func(Tx) T // creates a new domain transaction
	sync.Mutex
}

// Open creates a new database-backed repository.
// Creates the database schema if necessary.
func Open[T any](rw *sql.DB, ro *sql.DB, newT func(Tx) T, driver string, pragma map[string]string) (*DB[T], error) {
	d := New(rw, ro, newT, driver)
	err := d.init(pragma)
	return d, err
}

// New creates a new database instance.
func New[T any](rw *sql.DB, ro *sql.DB, newT func(Tx) T, driver string) *DB[T] {
	// Set the PostgreSQL flag if the driver is postgres
	SetPostgres(driver == DriverPostgres)

	return &DB[T]{
		RW:     rw,
		RO:     ro,
		Driver: driver,
		newT:   newT,
	}
}

// Update executes a function within a writable transaction.
func (d *DB[T]) Update(f func(tx T) error) error {
	return d.UpdateContext(context.Background(), f)
}

// UpdateContext executes a function within a writable transaction.
func (d *DB[T]) UpdateContext(ctx context.Context, f func(tx T) error) error {
	return d.execTx(ctx, true, f)
}

// View executes a function within a read-only transaction.
func (d *DB[T]) View(f func(tx T) error) error {
	return d.ViewContext(context.Background(), f)
}

// ViewContext executes a function within a read-only transaction.
func (d *DB[T]) ViewContext(ctx context.Context, f func(tx T) error) error {
	return d.execTx(ctx, false, f)
}

// Init sets the connection properties and creates the necessary tables.
func (d *DB[T]) init(pragma map[string]string) error {
	d.setNumConns()
	err := d.applySettings(pragma)
	if err != nil {
		return err
	}
	return d.createSchema()
}

// setNumConns sets the number of connections.
func (d *DB[T]) setNumConns() {
	// For the read-only DB handle the number of open connections
	// should be equal to the number of idle connections. Otherwise,
	// the handle will keep opening and closing connections, severely
	// impacting the througput.
	//
	// Benchmarks show that setting nConns>2 does not significantly
	// improve throughput, so I'm not sure what the best value is.
	// For now, I'm setting it to 2-8 depending on the number of CPUs.
	nConns := suggestNumConns()
	d.RO.SetMaxOpenConns(nConns)
	d.RO.SetMaxIdleConns(nConns)

	// SQLite allows only one writer at a time. Setting the maximum
	// number of DB connections to 1 for the read-write DB handle
	// is the best and fastest way to enforce this.
	d.RW.SetMaxOpenConns(1)
}

// applySettings applies the database settings.
func (d *DB[T]) applySettings(pragma map[string]string) error {
	// Skip if pragmas are empty
	if len(pragma) == 0 {
		return nil
	}

	// Skip for PostgreSQL - pragmas only apply to SQLite
	if d.Driver == DriverPostgres {
		return nil
	}

	// Ideally, we'd only set the pragmas in the connection string
	// (see [DataSource]), so we wouldn't need this function.
	// But since the mattn driver does not support setting pragmas
	// in the connection string, we also set them here.
	//
	// The correct way to set pragmas for the mattn driver is to
	// use the connection hook (see cmd/redka/main.go on how to do this).
	// But since we can't be sure the user does that, we also set them here.
	//
	// Unfortunately, setting pragmas using Exec only sets them for
	// a single connection. It's not a problem for d.RW (which has only
	// one connection), but it is for d.RO (which has multiple connections).
	// Still, it's better than nothing.
	//
	// See https://github.com/nalgeon/redka/issues/28 for more details.

	var query strings.Builder
	for name, val := range pragma {
		query.WriteString("pragma ")
		query.WriteString(name)
		query.WriteString("=")
		query.WriteString(val)
		query.WriteString(";")
	}
	if _, err := d.RW.Exec(query.String()); err != nil {
		return err
	}
	if _, err := d.RO.Exec(query.String()); err != nil {
		return err
	}
	return nil
}

// createSchema creates the database schema.
func (d *DB[T]) createSchema() error {
	// Use the appropriate schema based on the driver
	var schema string
	if d.Driver == DriverPostgres {
		// Use the embedded PostgreSQL schema
		schema = postgresSchema
	} else {
		// Use the default embedded SQLite schema
		schema = sqlSchema
	}

	if schema == "" {
		return nil
	}

	_, err := d.RW.Exec(schema)
	return err
}

// execTx executes a function within a transaction.
func (d *DB[T]) execTx(ctx context.Context, writable bool, f func(tx T) error) error {
	var dtx *sql.Tx
	var err error

	// For PostgreSQL read-only transactions, we need to explicitly set the transaction mode
	var opts *sql.TxOptions
	if d.Driver == DriverPostgres && !writable {
		opts = &sql.TxOptions{ReadOnly: true}
	}

	if writable {
		dtx, err = d.RW.BeginTx(ctx, opts)
	} else {
		dtx, err = d.RO.BeginTx(ctx, opts)
	}

	if err != nil {
		return err
	}
	defer func() { _ = dtx.Rollback() }()

	// Create a wrapper for *sql.Tx if we're using PostgreSQL
	var tx T
	if d.Driver == DriverPostgres {
		// Wrap the transaction with PostgreSQL query adaptation
		ptx := &PostgresTx{tx: dtx}
		tx = d.newT(ptx)
	} else {
		tx = d.newT(dtx)
	}

	err = f(tx)
	if err != nil {
		return err
	}
	return dtx.Commit()
}

// PostgresTx wraps a *sql.Tx to adapt queries for PostgreSQL
type PostgresTx struct {
	tx *sql.Tx
}

// Query adapts and executes a query for PostgreSQL
func (ptx *PostgresTx) Query(query string, args ...any) (*sql.Rows, error) {
	query = AdaptPostgresQuery(query)
	// println(">>>>>>> " + query)
	return ptx.tx.Query(query, args...)
}

// QueryRow adapts and executes a query for PostgreSQL
func (ptx *PostgresTx) QueryRow(query string, args ...any) *sql.Row {
	query = AdaptPostgresQuery(query)
	// println(">>>>>>> " + query)
	return ptx.tx.QueryRow(query, args...)
}

// Exec adapts and executes a query for PostgreSQL
func (ptx *PostgresTx) Exec(query string, args ...any) (sql.Result, error) {
	query = AdaptPostgresQuery(query)
	// println(">>>>>>> " + query)
	return ptx.tx.Exec(query, args...)
}

// DataSource returns a database connection string.
// For SQLite, it returns a connection string for a read-only or read-write mode.
// For PostgreSQL, it returns the connection string as-is.
func DataSource(path string, driver string, writable bool, pragma map[string]string) string {
	// For PostgreSQL, just return the connection string as-is
	if driver == DriverPostgres {
		return path
	}

	// Below is the SQLite-specific logic
	var ds string

	// Parse the parameters.
	source, query, _ := strings.Cut(path, "?")
	params, _ := url.ParseQuery(query)

	if source == ":memory:" {
		// This is an in-memory database, it must have a shared cache.
		// https://www.sqlite.org/sharedcache.html#shared_cache_and_in_memory_databases
		ds = "file:redka"
		params.Set("mode", "memory")
		params.Set("cache", "shared")
	} else {
		// This is a file-based database, it must have a "file:" prefix
		// for setting parameters (https://www.sqlite.org/c3ref/open.html).
		ds = source
		if !strings.HasPrefix(ds, "file:") {
			ds = "file:" + ds
		}
	}

	// sql.DB is concurrent-safe, so we don't need SQLite mutexes.
	params.Set("_mutex", "no")

	// Set the connection mode (writable or read-only).
	if writable {
		// Enable IMMEDIATE transactions for writable databases.
		// https://www.sqlite.org/lang_transaction.html
		params.Set("_txlock", "immediate")
	} else if params.Get("mode") != "memory" {
		// Enable read-only mode for read-only databases
		// (except for in-memory databases, which are always writable).
		// https://www.sqlite.org/c3ref/open.html
		params.Set("mode", "ro")
	}

	// Apply the pragma settings.
	// Some drivers (modernc and ncruces) setting passing pragmas
	// in the connection string, so we add them here.
	// The mattn driver does not support this, so it'll just ignore them.
	// For mattn driver, we have to set the pragmas in the connection hook.
	// (see cmd/redka/main.go on how to do this).
	for name, val := range pragma {
		params.Add("_pragma", name+"="+val)
	}

	return ds + "?" + params.Encode()
}

// suggestNumConns calculates the optimal number
// of parallel connections to the database.
func suggestNumConns() int {
	ncpu := runtime.NumCPU()
	switch {
	case ncpu < 2:
		return 2
	case ncpu > 8:
		return 8
	default:
		return ncpu
	}
}

// isSQLite returns true if the database is SQLite
func (d *DB[T]) isSQLite() bool {
	return d.Driver == DriverSQLite || d.Driver == "sqlite3" || d.Driver == "mattn-sqlite3" || d.Driver == "redka"
}
