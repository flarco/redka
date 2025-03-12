// Package rkey is a database-backed key repository.
// It provides methods to interact with keys in the database.
package rkey

import (
	"database/sql"
	"time"

	"github.com/flarco/redka/internal/core"
	"github.com/flarco/redka/internal/sqlx"
)

// DB is a database-backed key repository.
// A key is a unique identifier for a data structure
// (string, list, hash, etc.). Use the key repository
// to manage all keys regardless of their type.
type DB struct {
	*sqlx.DB[*Tx]
}

// New connects to the key repository.
// Does not create the database schema.
func New(rw *sql.DB, ro *sql.DB) *DB {
	d := sqlx.New(rw, ro, NewTx, sqlx.DriverSQLite)
	return &DB{d}
}

// NewWithDriver connects to the key repository with a specific driver.
// Does not create the database schema.
func NewWithDriver(rw *sql.DB, ro *sql.DB, driver string) *DB {
	d := sqlx.New(rw, ro, NewTx, driver)
	return &DB{d}
}

// Count returns the number of existing keys among specified.
func (db *DB) Count(keys ...string) (int, error) {
	tx := NewTx(db.RO)
	return tx.Count(keys...)
}

// Delete deletes keys and their values, regardless of the type.
// Returns the number of deleted keys. Non-existing keys are ignored.
func (db *DB) Delete(keys ...string) (int, error) {
	tx := NewTx(db.RW)
	return tx.Delete(keys...)
}

// DeleteAll deletes all keys and their values, effectively resetting
// the database. Should not be run inside a database transaction.
func (db *DB) DeleteAll() error {
	tx := NewTx(db.RW)
	return tx.DeleteAll()
}

// DeleteExpired deletes keys with expired TTL, but no more than n keys.
// If n = 0, deletes all expired keys.
func (db *DB) DeleteExpired(n int) (count int, err error) {
	tx := NewTx(db.RW)
	return tx.deleteExpired(n)
}

// Exists reports whether the key exists.
func (db *DB) Exists(key string) (bool, error) {
	tx := NewTx(db.RO)
	return tx.Exists(key)
}

// Expire sets a time-to-live (ttl) for the key using a relative duration.
// After the ttl passes, the key is expired and no longer exists.
// If the key does not exist, returns ErrNotFound.
func (db *DB) Expire(key string, ttl time.Duration) error {
	tx := NewTx(db.RW)
	return tx.Expire(key, ttl)
}

// ExpireAt sets an expiration time for the key. After this time,
// the key is expired and no longer exists.
// If the key does not exist, returns ErrNotFound.
func (db *DB) ExpireAt(key string, at time.Time) error {
	tx := NewTx(db.RW)
	return tx.ExpireAt(key, at)
}

// Get returns a specific key with all associated details.
// If the key does not exist, returns ErrNotFound.
func (db *DB) Get(key string) (core.Key, error) {
	tx := NewTx(db.RO)
	return tx.Get(key)
}

// Keys returns all keys matching pattern.
// Supports glob-style patterns like these:
//
//	key*  k?y  k[bce]y  k[!a-c][y-z]
//
// Use this method only if you are sure that the number of keys is
// limited. Otherwise, use the [DB.Scan] or [DB.Scanner] methods.
func (db *DB) Keys(pattern string) ([]core.Key, error) {
	tx := NewTx(db.RO)
	return tx.Keys(pattern)
}

// Len returns the total number of keys, including expired ones.
func (db *DB) Len() (int, error) {
	tx := NewTx(db.RO)
	return tx.Len()
}

// Persist removes the expiration time for the key.
// If the key does not exist, returns ErrNotFound.
func (db *DB) Persist(key string) error {
	tx := NewTx(db.RW)
	return tx.Persist(key)
}

// Random returns a random key.
// If there are no keys, returns ErrNotFound.
func (db *DB) Random() (core.Key, error) {
	tx := NewTx(db.RO)
	return tx.Random()
}

// Rename changes the key name.
// If there is an existing key with the new name, it is replaced.
// If the old key does not exist, returns ErrNotFound.
func (db *DB) Rename(key, newKey string) error {
	err := db.Update(func(tx *Tx) error {
		err := tx.Rename(key, newKey)
		return err
	})
	return err
}

// RenameNotExists changes the key name.
// If there is an existing key with the new name, does nothing.
// Returns true if the key was renamed, false otherwise.
func (db *DB) RenameNotExists(key, newKey string) (bool, error) {
	var ok bool
	err := db.Update(func(tx *Tx) error {
		var err error
		ok, err = tx.RenameNotExists(key, newKey)
		return err
	})
	return ok, err
}

// Scan iterates over keys matching pattern.
// Returns a slice of keys (see [core.Key]) of size count
// based on the current state of the cursor.
// Returns an empty slice when there are no more keys.
//
// Filtering and limiting options:
//   - pattern (glob-style) to filter keys by name (* = any name).
//   - ktype to filter keys by type (TypeAny = any type).
//   - count to limit the number of keys returned (0 = default).
func (db *DB) Scan(cursor int, pattern string, ktype core.TypeID, count int) (ScanResult, error) {
	tx := NewTx(db.RO)
	return tx.Scan(cursor, pattern, ktype, count)
}

// Scanner returns an iterator for keys matching pattern.
// The scanner returns keys one by one, fetching them
// from the database in pageSize batches when necessary.
// Stops when there are no more items or an error occurs.
//
// Filtering and pagination options:
//   - pattern (glob-style) to filter keys by name (* = any name).
//   - ktype to filter keys by type (TypeAny = any type).
//   - pageSize to limit the number of keys fetched at once (0 = default).
func (db *DB) Scanner(pattern string, ktype core.TypeID, pageSize int) *Scanner {
	return newScanner(NewTx(db.RO), pattern, ktype, pageSize)
}
