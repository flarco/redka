package redka_test

import (
	"database/sql"
	"errors"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
)

// Test constants
const (
	// Default values for local development
	envPostgresHost     = "POSTGRES_HOST"
	envPostgresPort     = "POSTGRES_PORT"
	envPostgresUser     = "POSTGRES_USER"
	envPostgresPassword = "POSTGRES_PASSWORD"
	envPostgresDB       = "POSTGRES_DB"
	envPostgresSchema   = "POSTGRES_SCHEMA"

	defaultPostgresHost     = "localhost"
	defaultPostgresPort     = "5433"
	defaultPostgresUser     = "postgres"
	defaultPostgresPassword = "postgres"
	defaultPostgresDB       = "redka_test"
	defaultPostgresSchema   = "public"
)

// getPostgresConnectionString returns a PostgreSQL connection string
// using environment variables or default values
func getPostgresConnectionString() string {
	if pgConnString := getEnvOrDefault("POSTGRES_REDKA_CONN_STRING", ""); pgConnString != "" {
		return pgConnString
	}

	host := getEnvOrDefault(envPostgresHost, defaultPostgresHost)
	port := getEnvOrDefault(envPostgresPort, defaultPostgresPort)
	user := getEnvOrDefault(envPostgresUser, defaultPostgresUser)
	password := getEnvOrDefault(envPostgresPassword, defaultPostgresPassword)
	dbname := getEnvOrDefault(envPostgresDB, defaultPostgresDB)
	schemaName := getEnvOrDefault(envPostgresSchema, defaultPostgresSchema)

	return "host=" + host + " port=" + port + " user=" + user +
		" password=" + password + " dbname=" + dbname + " search_path=" + schemaName + " sslmode=disable"
}

// getEnvOrDefault returns environment variable value or default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// getPostgresDB creates and returns a new database connection for testing
func getPostgresDB(tb testing.TB) *redka.DB {
	tb.Helper()

	connStr := getPostgresConnectionString()

	// Create options for PostgreSQL
	opts := &redka.Options{
		DriverName: "postgres",
	}

	db, err := redka.Open(connStr, opts)
	if err != nil {
		tb.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}

	// Clear the database before each test
	err = clearPostgresDB(db)
	if err != nil {
		tb.Fatalf("Failed to clear PostgreSQL database: %v", err)
	}

	return db
}

// clearPostgresDB removes all data from test tables
func clearPostgresDB(db *redka.DB) error {
	tables := []string{"rzset", "rhash", "rset", "rlist", "rstring", "rkey"}

	for _, table := range tables {
		_, err := db.RW.Exec("DELETE FROM " + table)
		if err != nil {
			return err
		}
	}

	return nil
}

// skipIfNoPostgres skips the test if PostgreSQL is not available
func skipIfNoPostgres(t *testing.T) {
	connStr := getPostgresConnectionString()
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Skip("Skipping PostgreSQL tests: " + err.Error())
	}

	err = db.Ping()
	if err != nil {
		t.Skip("Skipping PostgreSQL tests, could not connect: " + err.Error())
	}

	db.Close()
}

// TestPostgresConnection tests basic connection to PostgreSQL
func TestPostgresConnection(t *testing.T) {
	skipIfNoPostgres(t)

	db := getPostgresDB(t)
	defer db.Close()

	err := db.Str().Set("name", "alice")
	if err != nil {
		t.Fatalf("Failed to set string: %v", err)
	}

	val, err := db.Str().Get("name")
	if err != nil {
		t.Fatalf("Failed to get string: %v", err)
	}

	if val.String() != "alice" {
		t.Errorf("Expected 'alice', got '%s'", val.String())
	}
}

// TestPostgresView tests a read-only transaction with PostgreSQL
func TestPostgresView(t *testing.T) {
	skipIfNoPostgres(t)

	db := getPostgresDB(t)
	defer db.Close()

	// Set some initial data
	err := db.Str().Set("name", "bob")
	if err != nil {
		t.Fatalf("Failed to set string: %v", err)
	}

	// Test view transaction
	var name core.Value
	err = db.View(func(tx *redka.Tx) error {
		val, err := tx.Str().Get("name")
		if err != nil {
			return err
		}
		name = val
		return nil
	})

	if err != nil {
		t.Fatalf("View transaction failed: %v", err)
	}

	if name.String() != "bob" {
		t.Errorf("Expected 'bob', got '%s'", name.String())
	}
}

// TestPostgresUpdate tests a read-write transaction with PostgreSQL
func TestPostgresUpdate(t *testing.T) {
	skipIfNoPostgres(t)

	db := getPostgresDB(t)
	defer db.Close()

	// Test update transaction
	err := db.Update(func(tx *redka.Tx) error {
		err := tx.Str().Set("city", "Paris")
		if err != nil {
			return err
		}

		val, err := tx.Str().Get("city")
		if err != nil {
			return err
		}

		if val.String() != "Paris" {
			return errors.New("value mismatch in transaction")
		}

		return nil
	})

	if err != nil {
		t.Fatalf("Update transaction failed: %v", err)
	}

	// Verify outside the transaction
	val, err := db.Str().Get("city")
	if err != nil {
		t.Fatalf("Failed to get string: %v", err)
	}

	if val.String() != "Paris" {
		t.Errorf("Expected 'Paris', got '%s'", val.String())
	}
}

// TestPostgresUpdateRollback tests transaction rollback with PostgreSQL
func TestPostgresUpdateRollback(t *testing.T) {
	skipIfNoPostgres(t)

	db := getPostgresDB(t)
	defer db.Close()

	// Set initial data
	err := db.Str().Set("country", "France")
	if err != nil {
		t.Fatalf("Failed to set string: %v", err)
	}

	// Expected error to trigger rollback
	expectedError := errors.New("forcing rollback")

	// Test update transaction with rollback
	err = db.Update(func(tx *redka.Tx) error {
		err := tx.Str().Set("country", "Germany")
		if err != nil {
			return err
		}

		return expectedError // Force rollback
	})

	if err != expectedError {
		t.Fatalf("Expected specific error but got: %v", err)
	}

	// Verify the value was not changed (rolled back)
	val, err := db.Str().Get("country")
	if err != nil {
		t.Fatalf("Failed to get string: %v", err)
	}

	if val.String() != "France" {
		t.Errorf("Expected 'France' (rollback succeeded), got '%s'", val.String())
	}
}

// TestPostgresHash tests hash operations with PostgreSQL
func TestPostgresHash(t *testing.T) {
	skipIfNoPostgres(t)

	db := getPostgresDB(t)
	defer db.Close()

	// Test hash operations
	created, err := db.Hash().Set("user:1", "name", "charlie")
	if err != nil {
		t.Fatalf("Failed to set hash: %v", err)
	}

	if !created {
		t.Errorf("Expected field to be created")
	}

	created, err = db.Hash().Set("user:1", "age", "30")
	if err != nil {
		t.Fatalf("Failed to set hash: %v", err)
	}

	if !created {
		t.Errorf("Expected field to be created")
	}

	val, err := db.Hash().Get("user:1", "name")
	if err != nil {
		t.Fatalf("Failed to get hash: %v", err)
	}

	if val.String() != "charlie" {
		t.Errorf("Expected 'charlie', got '%s'", val.String())
	}

	// Test hash exists
	exists, err := db.Hash().Exists("user:1", "age")
	if err != nil {
		t.Fatalf("Failed to check hash exists: %v", err)
	}

	if !exists {
		t.Errorf("Expected field 'age' to exist")
	}

	// Test hash fields
	fields, err := db.Hash().Fields("user:1")
	if err != nil {
		t.Fatalf("Failed to get hash fields: %v", err)
	}

	if len(fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(fields))
	}
}

// TestPostgresList tests list operations with PostgreSQL
func TestPostgresList(t *testing.T) {
	skipIfNoPostgres(t)

	db := getPostgresDB(t)
	defer db.Close()

	// Test list operations
	count, err := db.List().PushBack("colors", "red")
	if err != nil {
		t.Fatalf("Failed to push to list: %v", err)
	}

	count, err = db.List().PushBack("colors", "green")
	if err != nil {
		t.Fatalf("Failed to push to list: %v", err)
	}

	count, err = db.List().PushBack("colors", "blue")
	if err != nil {
		t.Fatalf("Failed to push to list: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected list length 3, got %d", count)
	}

	// Test list length
	length, err := db.List().Len("colors")
	if err != nil {
		t.Fatalf("Failed to get list length: %v", err)
	}

	if length != 3 {
		t.Errorf("Expected list length 3, got %d", length)
	}

	// Test list range
	items, err := db.List().Range("colors", 0, -1)
	if err != nil {
		t.Fatalf("Failed to get list range: %v", err)
	}

	if len(items) != 3 {
		t.Errorf("Expected 3 items, got %d", len(items))
	}

	if items[0].String() != "red" || items[1].String() != "green" || items[2].String() != "blue" {
		t.Errorf("List items don't match expected values")
	}
}

// TestPostgresSet tests set operations with PostgreSQL
func TestPostgresSet(t *testing.T) {
	skipIfNoPostgres(t)

	db := getPostgresDB(t)
	defer db.Close()

	// Test set operations
	count, err := db.Set().Add("fruits", "apple", "banana", "cherry")
	if err != nil {
		t.Fatalf("Failed to add to set: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected 3 members added, got %d", count)
	}

	// Test set members
	members, err := db.Set().Items("fruits")
	if err != nil {
		t.Fatalf("Failed to get set members: %v", err)
	}

	if len(members) != 3 {
		t.Errorf("Expected 3 members, got %d", len(members))
	}

	// Test is member
	isMember, err := db.Set().Exists("fruits", "banana")
	if err != nil {
		t.Fatalf("Failed to check set membership: %v", err)
	}

	if !isMember {
		t.Errorf("Expected 'banana' to be a member")
	}
}

// TestPostgresZSet tests sorted set operations with PostgreSQL
func TestPostgresZSet(t *testing.T) {
	skipIfNoPostgres(t)

	db := getPostgresDB(t)
	defer db.Close()

	// Test sorted set operations
	added, err := db.ZSet().Add("scores", "player1", 10.5)
	if err != nil {
		t.Fatalf("Failed to add to sorted set: %v", err)
	}

	if !added {
		t.Errorf("Expected element to be added")
	}

	added, err = db.ZSet().Add("scores", "player2", 20.3)
	if err != nil {
		t.Fatalf("Failed to add to sorted set: %v", err)
	}

	if !added {
		t.Errorf("Expected element to be added")
	}

	added, err = db.ZSet().Add("scores", "player3", 15.7)
	if err != nil {
		t.Fatalf("Failed to add to sorted set: %v", err)
	}

	if !added {
		t.Errorf("Expected element to be added")
	}

	// Test range by score
	items, err := db.ZSet().RangeWith("scores").ByScore(10, 16).Run()
	if err != nil {
		t.Fatalf("Failed to get range by score: %v", err)
	}

	if len(items) != 2 {
		t.Errorf("Expected 2 members, got %d", len(items))
	}

	// Check if the members are in the correct order
	if items[0].Elem.String() != "player1" || items[1].Elem.String() != "player3" {
		t.Errorf("Members not in expected order")
	}

	// Test score
	score, err := db.ZSet().GetScore("scores", "player2")
	if err != nil {
		t.Fatalf("Failed to get score: %v", err)
	}

	if score != 20.3 {
		t.Errorf("Expected score 20.3, got %f", score)
	}
}
