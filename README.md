<img alt="Redka" src="logo.svg" height="80" align="center">

Redka aims to reimplement the core parts of Redis with SQLite, while remaining compatible with Redis API.

Notable features:

-   Data does not have to fit in RAM.
-   ACID transactions.
-   SQL views for better introspection and reporting.
-   Both in-process (Go API) and standalone (RESP) servers.
-   Redis-compatible commands and wire protocol.
-   Support for both SQLite and PostgreSQL backends.

Redka is [functionally ready](docs/roadmap.md) for 1.0. Feel free to try it in non-critical production scenarios and provide feedback in the issues.

## Commands

Redka supports five core Redis data types:

-   [Strings](docs/commands/strings.md) are the most basic Redis type, representing a sequence of bytes.
-   [Lists](docs/commands/lists.md) are sequences of strings sorted by insertion order.
-   [Sets](docs/commands/sets.md) are unordered collections of unique strings.
-   [Hashes](docs/commands/hashes.md) are field-value (hash)maps.
-   [Sorted sets](docs/commands/sorted-sets.md) (zsets) are collections of unique strings ordered by each string's associated score.

Redka also provides commands for [key management](docs/commands/keys.md), [server/connection management](docs/commands/server.md), and [transactions](docs/commands/transactions.md).

## Installation and usage

Redka comes in two flavors:

-   Standalone Redis-compatible server: [installation](docs/install-standalone.md), [usage](docs/usage-standalone.md).
-   Go module for in-process use: [installation](docs/install-module.md), [usage](docs/usage-module.md).

## Database Backend

Redka supports two database backends:

- **SQLite** (default): Great for single-node deployments, embedded applications, and when you need a self-contained database file.
- **PostgreSQL**: Ideal for distributed setups, when you need better concurrency, or want to leverage PostgreSQL's robust features.

### Using PostgreSQL

To use PostgreSQL instead of SQLite:

```go
import (
    "github.com/flarco/redka"
    _ "github.com/lib/pq"
)

func main() {
    // PostgreSQL connection string
    connStr := "host=localhost port=5432 user=postgres password=postgres dbname=redka sslmode=disable"
    
    // Create options with PostgreSQL driver
    opts := &redka.Options{
        DriverName: "postgres",
    }
    
    // Open the database
    db, err := redka.Open(connStr, opts)
    if err != nil {
        panic(err)
    }
    defer db.Close()
    
    // Use the same Redka API as with SQLite
    err = db.Str().Set("name", "alice")
    // ...
}
```

For testing with PostgreSQL, you can use the included Docker Compose configuration:

```bash
# Start PostgreSQL
docker-compose up -d postgres

# Run tests
go test -v ./... -tags=postgres
```

## Performance

According to the [benchmarks](docs/performance.md), Redka is several times slower than Redis. Still, it can do up to 100K op/sec on a Macbook Air, which is pretty good if you ask me (and probably 10x more than most applications will ever need).

Redka stores data in a [SQLite database](docs/persistence.md) with a simple schema and provides views for better introspection. With PostgreSQL backend, you get improved concurrency at the cost of slightly lower raw performance.

## Contributing

Contributions are welcome. For anything other than bugfixes, please first open an issue to discuss what you want to change.

Be sure to add or update tests as appropriate.

## Acknowledgements

Redka would not be possible without these great projects and their creators:

-   [Redis](https://redis.io/) ([Salvatore Sanfilippo](https://github.com/antirez)). It's such an amazing idea to go beyond the get-set paradigm and provide a convenient API for more complex data structures.
-   [SQLite](https://sqlite.org/) ([D. Richard Hipp](https://www.sqlite.org/crew.html)). The in-process database powering the world.
-   [Redcon](https://github.com/tidwall/redcon) ([Josh Baker](https://github.com/tidwall)). A very clean and convenient implementation of a RESP server.

Logo font by [Ek Type](https://ektype.in/).

## Support

Redka is mostly a [one-man](https://antonz.org/) project, not backed by a VC fund or anything.

If you find Redka useful, please star it on GitHub and spread the word among your peers. It really helps to move the project forward.

If you use Redka for commercial purposes, consider [purchasing support](https://antonz.gumroad.com/l/redka-plus).

★ [Subscribe](https://antonz.org/subscribe/) to stay on top of new features.
