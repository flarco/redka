// A basic example of using Redka
// with github.com/mattn/go-sqlite3 driver.
package main

import (
	"log"
	"log/slog"

	"github.com/flarco/redka"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	// Open a database.
	db, err := redka.Open("data.db", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Set some string keys.
	err = db.Str().Set("name", "alice")
	slog.Info("set", "err", err)
	err = db.Str().Set("age", 25)
	slog.Info("set", "err", err)

	// Check if the keys exist.
	count, err := db.Key().Count("name", "age", "city")
	slog.Info("count", "count", count, "err", err)

	// Get a key.
	name, err := db.Str().Get("name")
	slog.Info("get", "name", name, "err", err)
}
