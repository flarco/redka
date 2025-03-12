// Package sqlx provides base types and helper functions
// to work with SQL databases.
package sqlx

import (
	"database/sql"
	"strconv"
	"strings"

	"github.com/flarco/redka/internal/core"
)

// Sorting direction.
const (
	Asc  = "asc"
	Desc = "desc"
)

// Aggregation functions.
const (
	Sum = "sum"
	Min = "min"
	Max = "max"
)

// Database types
var (
	isPostgres bool
)

// SetPostgres sets whether the database is PostgreSQL.
func SetPostgres(postgres bool) {
	isPostgres = postgres
}

// IsPostgres returns whether the database is PostgreSQL.
func IsPostgres() bool {
	return isPostgres
}

// Tx is a database transaction (or a transaction-like object).
type Tx interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
}

// rowScanner is an interface to scan rows.
type RowScanner interface {
	Scan(dest ...any) error
}

// ExpandIn expands the IN clause in the query for a given parameter.
func ExpandIn[T any](query string, param string, args []T) (string, []any) {
	anyArgs := make([]any, len(args))
	pholders := make([]string, len(args))
	for i, arg := range args {
		anyArgs[i] = arg
		pholders[i] = "?"
	}
	query = strings.Replace(query, param, strings.Join(pholders, ","), 1)
	return query, anyArgs
}

func Select[T any](db Tx, query string, args []any,
	scan func(rows *sql.Rows) (T, error)) ([]T, error) {

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vals []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return vals, err
}

// Returns typed errors for some specific cases.
func TypedError(err error) error {
	if err == nil {
		return nil
	}
	if ConstraintFailed(err, "NOT NULL", "rkey.type") {
		return core.ErrKeyType
	}
	return err
}

// ConstraintFailed checks if the error is due to
// a constraint violation on a column.
func ConstraintFailed(err error, constraint, column string) bool {
	msg := constraint + " constraint failed: " + column
	return strings.Contains(err.Error(), msg)
}

// AdaptPostgresQuery adapts a SQLite query to work with PostgreSQL
func AdaptPostgresQuery(query string) string {
	// Replace SQLite-specific syntax with PostgreSQL syntax

	// Replace "update or replace" with standard PostgreSQL syntax
	if strings.Contains(query, "update or replace") {
		query = strings.ReplaceAll(query, "update or replace", "update")
	}

	// Replace rowid references with PostgreSQL-compatible column
	query = strings.ReplaceAll(query, "rowid", "ctid::text")

	// Replace GLOB with ILIKE for pattern matching (case-insensitive like in PostgreSQL)
	query = strings.ReplaceAll(query, " glob ", " ILIKE ")
	query = strings.ReplaceAll(query, "field glob", "field ILIKE")
	query = strings.ReplaceAll(query, "elem glob", "elem ILIKE")

	// Fix IN clause with empty lists - PostgreSQL doesn't like empty IN clauses
	if strings.Contains(query, "in ()") {
		query = strings.ReplaceAll(query, "in ()", "= ANY('{}'::text[])")
	}

	// Fix ambiguous column references in ON CONFLICT clauses
	if strings.Contains(query, "ON CONFLICT") || strings.Contains(query, "on conflict") {
		// Fix the ambiguous "type" reference in DO UPDATE SET clauses
		if strings.Contains(query, "type = CASE WHEN type = excluded.type") {
			query = strings.Replace(query,
				"type = CASE WHEN type = excluded.type THEN type ELSE null END",
				"type = CASE WHEN rkey.type = excluded.type THEN rkey.type ELSE null END", -1)
		}
		if strings.Contains(query, "type = case when type = excluded.type") {
			query = strings.Replace(query,
				"type = case when type = excluded.type then type else null end",
				"type = CASE WHEN rkey.type = excluded.type THEN rkey.type ELSE null END", -1)
		}

		// Fix ambiguous "version" column references
		if strings.Contains(query, "version = version+1") {
			query = strings.Replace(query,
				"version = version+1",
				"version = rkey.version+1", -1)
		}
		if strings.Contains(query, "version = version + 1") {
			query = strings.Replace(query,
				"version = version + 1",
				"version = rkey.version + 1", -1)
		}

		// Fix ambiguous "mtime" column references
		if strings.Contains(query, "mtime = excluded.mtime") {
			query = strings.Replace(query,
				"mtime = excluded.mtime",
				"mtime = excluded.mtime", -1) // This is fine as is since one is qualified
		}

		// Fix ambiguous "etime" column references
		if strings.Contains(query, "etime = excluded.etime") {
			query = strings.Replace(query,
				"etime = excluded.etime",
				"etime = excluded.etime", -1) // This is fine as is since one is qualified
		}

		// Fix ambiguous "len" column references
		if strings.Contains(query, "len = len + 1") {
			query = strings.Replace(query,
				"len = len + 1",
				"len = rkey.len + 1", -1)
		}
	}

	// Convert SQLite ON CONFLICT clause to PostgreSQL format
	if strings.Contains(query, "on conflict") {
		// Handle RETURNING clause for PostgreSQL - must place after the DO UPDATE/NOTHING
		if strings.Contains(query, "returning") {
			// Remove the RETURNING clause temporarily
			var returningClause string
			parts := strings.Split(query, "returning")
			if len(parts) > 1 {
				query = parts[0]
				returningClause = "RETURNING" + parts[1]

				// Add the RETURNING clause at the end after fixing ON CONFLICT
				if strings.Contains(query, "on conflict") {
					// Fix PostgreSQL ON CONFLICT DO UPDATE syntax
					query = strings.Replace(query,
						"on conflict",
						"ON CONFLICT", -1)

					query = strings.Replace(query,
						"do update set",
						"DO UPDATE SET", -1)

					// Add the RETURNING clause at the end
					query = query + " " + returningClause
				}
			}
		} else {
			// Fix PostgreSQL ON CONFLICT DO UPDATE syntax without RETURNING
			query = strings.Replace(query,
				"on conflict",
				"ON CONFLICT", -1)

			query = strings.Replace(query,
				"do update set",
				"DO UPDATE SET", -1)

			query = strings.Replace(query,
				"do nothing",
				"DO NOTHING", -1)
		}
	}

	// Fix CASE expressions for PostgreSQL (make them uppercase)
	if strings.Contains(query, "case when") {
		query = strings.Replace(query,
			"case when",
			"CASE WHEN", -1)

		query = strings.Replace(query,
			"then",
			"THEN", -1)

		query = strings.Replace(query,
			"else",
			"ELSE", -1)

		query = strings.Replace(query,
			"end",
			"END", -1)
	}

	// Fix limit clause syntax by adding OFFSET keyword for PostgreSQL
	if strings.Contains(query, "limit\n") {
		query = strings.ReplaceAll(query, "limit\n", "LIMIT ")
	}

	// Another way to handle LIMIT with comma syntax in PostgreSQL (LIMIT x, y becomes LIMIT y OFFSET x)
	if strings.Contains(query, "limit") {
		// Check for the pattern "limit X, Y" which needs to be "limit Y offset X" in PostgreSQL
		limStartIdx := strings.Index(query, "limit")
		if limStartIdx != -1 {
			limEndIdx := -1
			restOfQuery := query[limStartIdx:]
			semicolonPos := strings.Index(restOfQuery, ";")
			if semicolonPos != -1 {
				limEndIdx = limStartIdx + semicolonPos
			} else {
				limEndIdx = len(query)
			}

			limitClause := query[limStartIdx:limEndIdx]
			if strings.Contains(limitClause, ",") {
				// Split by comma to get X and Y
				parts := strings.Split(limitClause[6:], ",")
				if len(parts) == 2 {
					x := strings.TrimSpace(parts[0])
					y := strings.TrimSpace(parts[1])
					newLimitClause := "LIMIT " + y + " OFFSET " + x
					query = query[:limStartIdx] + newLimitClause + query[limEndIdx:]
				}
			}
		}
	}

	// Fix SQL keywords to uppercase for better readability and consistency
	query = strings.ReplaceAll(query, "and field in (:fields)", "AND field IN (:fields)")
	query = strings.ReplaceAll(query, "and field = ?", "AND field = ?")
	query = strings.ReplaceAll(query, "and (", "AND (")
	query = strings.ReplaceAll(query, "and elem in", "AND elem IN")
	query = strings.ReplaceAll(query, "and elem = ?", "AND elem = ?")
	query = strings.ReplaceAll(query, "and score between", "AND score BETWEEN")
	query = strings.ReplaceAll(query, "and ", "AND ")
	query = strings.ReplaceAll(query, " and ", " AND ")
	query = strings.ReplaceAll(query, "\nand ", "\nAND ")
	query = strings.ReplaceAll(query, "and\n", "AND\n")
	query = strings.ReplaceAll(query, "or ", "OR ")
	query = strings.ReplaceAll(query, " or ", " OR ")
	query = strings.ReplaceAll(query, "\nor ", "\nOR ")
	query = strings.ReplaceAll(query, "or\n", "OR\n")
	query = strings.ReplaceAll(query, "where ", "WHERE ")
	query = strings.ReplaceAll(query, " where ", " WHERE ")
	query = strings.ReplaceAll(query, "\nwhere ", "\nWHERE ")
	query = strings.ReplaceAll(query, "join ", "JOIN ")
	query = strings.ReplaceAll(query, " join ", " JOIN ")
	query = strings.ReplaceAll(query, "\njoin ", "\nJOIN ")
	query = strings.ReplaceAll(query, "order by ", "ORDER BY ")
	query = strings.ReplaceAll(query, " order by ", " ORDER BY ")
	query = strings.ReplaceAll(query, "\norder by ", "\nORDER BY ")
	query = strings.ReplaceAll(query, "group by ", "GROUP BY ")
	query = strings.ReplaceAll(query, " group by ", " GROUP BY ")
	query = strings.ReplaceAll(query, "\ngroup by ", "\nGROUP BY ")

	// Fix ambiguous column references in JOINs for PostgreSQL
	// PostgreSQL requires full qualification of columns when they appear in multiple tables
	if strings.Contains(query, "JOIN") {
		// Simple replacement for the most common type checks
		if strings.Contains(query, "type = 1") {
			query = strings.Replace(query, "type = 1", "rkey.type = 1", 1)
		}
		if strings.Contains(query, "type = 2") {
			query = strings.Replace(query, "type = 2", "rkey.type = 2", 1)
		}
		if strings.Contains(query, "type = 3") {
			query = strings.Replace(query, "type = 3", "rkey.type = 3", 1)
		}
		if strings.Contains(query, "type = 4") {
			query = strings.Replace(query, "type = 4", "rkey.type = 4", 1)
		}
		if strings.Contains(query, "type = 5") {
			query = strings.Replace(query, "type = 5", "rkey.type = 5", 1)
		}

		// Qualify ambiguous column references in JOIN conditions
		query = strings.Replace(query, "on kid = rkey.id and", "on kid = rkey.id AND", -1)

		// Qualify etime and other ambiguous columns in WHERE clauses
		if strings.Contains(query, "WHERE key =") && strings.Contains(query, "etime is null") {
			query = strings.Replace(query, "etime is null", "rkey.etime is null", -1)
		}
		if strings.Contains(query, "WHERE key =") && strings.Contains(query, "etime >") {
			query = strings.Replace(query, "etime >", "rkey.etime >", -1)
		}
	}

	// Convert ? placeholders to $1, $2, $3 format for PostgreSQL
	query = ConvertPlaceholders(query)

	return query
}

func ConvertPlaceholders(query string) string {
	// Convert ? placeholders to $1, $2, $3 format for PostgreSQL
	// This is a critical difference between SQLite and PostgreSQL
	if strings.Contains(query, "?") {
		paramNum := 1
		var newQuery strings.Builder

		for i := 0; i < len(query); i++ {
			if query[i] == '?' {
				newQuery.WriteString("$" + strconv.Itoa(paramNum))
				paramNum++
			} else {
				newQuery.WriteByte(query[i])
			}
		}

		query = newQuery.String()
	}

	return query

}
