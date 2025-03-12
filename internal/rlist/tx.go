package rlist

import (
	"database/sql"
	"strings"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

const (
	sqlDelete = `
	delete from rlist
	where kid = (
			select id from rkey
			where key = ? and type = 2 and (etime is null or etime > ?)
		) and elem = ?`

	sqlDeleteBack = `
	with ids as (
		select rlist.rowid
		from rlist join rkey on kid = rkey.id and type = 2
		where key = ? and (etime is null or etime > ?) and elem = ?
		order by pos desc
		limit ?
	)
	delete from rlist
	where rowid in (select rowid from ids)`

	sqlDeleteFront = `
	with ids as (
		select rlist.rowid
		from rlist join rkey on kid = rkey.id and type = 2
		where key = ? and (etime is null or etime > ?) and elem = ?
		order by pos
		limit ?
	)
	delete from rlist
	where rowid in (select rowid from ids)`

	sqlGet = `
	with elems as (
		select elem, row_number() over (order by pos asc) as rownum
		from rlist join rkey on kid = rkey.id and type = 2
		where key = ? and (etime is null or etime > ?)
	)
	select elem
	from elems
	where rownum = ? + 1`

	sqlInsert = `
	update rkey set
		version = version + 1,
		mtime = ?,
		len = len + 1
	where key = ? and type = 2 and (etime is null or etime > ?)
	returning id, len`

	sqlInsertAfter = `
	with elprev as (
		select min(pos) as pos from rlist
		where kid = ? and elem = ?
	),
	elnext as (
		select min(pos) as pos from rlist
		where kid = ? and pos > (select pos from elprev)
	),
	newpos as (
		select
			case
				when elnext.pos is null then elprev.pos + 1
				else (elprev.pos + elnext.pos) / 2
			end as pos
		from elprev, elnext
	)
	insert into rlist (kid, pos, elem)
	select ?, (select pos from newpos), ?
	from rlist
	where kid = ?
	limit 1`

	sqlInsertBefore = `
	with elnext as (
		select min(pos) as pos from rlist
		where kid = ? and elem = ?
	),
	elprev as (
		select max(pos) as pos from rlist
		where kid = ? and pos < (select pos from elnext)
	),
	newpos as (
		select
			case
				when elprev.pos is null then elnext.pos - 1
				else (elprev.pos + elnext.pos) / 2
			end as pos
		from elprev, elnext
	)
	insert into rlist (kid, pos, elem)
	select ?, (select pos from newpos), ?
	from rlist
	where kid = ?
	limit 1`

	sqlLen = `
	select len from rkey
	where key = ? and type = 2 and (etime is null or etime > ?)`

	sqlPopBack = `
	with curkey as (
		select id from rkey
		where key = ? and type = 2 and (etime is null or etime > ?)
	)
	delete from rlist
	where
		kid = (select id from curkey)
		and pos = (
			select max(pos) from rlist
			where kid = (select id from curkey)
		)
	returning elem`

	sqlPopFront = `
	with curkey as (
		select id from rkey
		where key = ? and type = 2 and (etime is null or etime > ?)
	)
	delete from rlist
	where
		kid = (select id from curkey)
		and pos = (
			select min(pos) from rlist
			where kid = (select id from curkey)
		)
	returning elem`

	sqlPush = `
	insert into
	rkey   (key, type, version, mtime, len)
	values (  ?,    2,       1,     ?,   1)
	on conflict (key) do update set
		type = case when type = excluded.type then type else null end,
		version = version + 1,
		mtime = excluded.mtime,
		len = len + 1
	returning id, len`

	sqlPushBack = `
	insert into rlist (kid, pos, elem)
	select ?, coalesce(max(pos)+1, 0), ?
	from rlist
	where kid = ?`

	sqlPushFront = `
	insert into rlist (kid, pos, elem)
	select ?, coalesce(min(pos)-1, 0), ?
	from rlist
	where kid = ?`

	sqlRange = `
	with curkey as (
		select id from rkey
		where key = ? and type = 2 and (etime is null or etime > ?)
	),
	counts as (
		select len from rkey
		where id = (select id from curkey)
	),
	bounds as (
		select
			case when ? < 0
				then (select len from counts) + ?
				else ?
			end as start,
			case when ? < 0
				then (select len from counts) + ?
				else ?
			end as stop
	)
	select elem
	from rlist
	where kid = (select id from curkey)
	order by pos
	limit
		(select start from bounds),
		((select stop from bounds) - (select start from bounds) + 1)`

	// PostgreSQL version of sqlRange with LIMIT/OFFSET syntax
	sqlRangePostgres = `
	with curkey as (
		select id from rkey
		where key = ? and type = 2 and (etime is null or etime > ?)
	),
	counts as (
		select len from rkey
		where id = (select id from curkey)
	),
	bounds as (
		select
			case when ? < 0
				then (select len from counts) + ?
				else ?
			end as start,
			case when ? < 0
				then (select len from counts) + ?
				else ?
			end as stop
	)
	select elem
	from rlist
	where kid = (select id from curkey)
	order by pos
	limit ((select stop from bounds) - (select start from bounds) + 1)
	offset (select start from bounds)`

	sqlSet = `
	with curkey as (
		select id from rkey
		where key = ? and type = 2 and (etime is null or etime > ?)
    ),
    elems as (
		select pos, row_number() over (order by pos asc) as rownum
		from rlist
		where kid = (select id from curkey)
    )
    update rlist set elem = ?
    where kid = (select id from curkey)
		and pos = (select pos from elems where rownum = ? + 1)`

	sqlTrim = `
	with curkey as (
		select id from rkey
		where key = ? and type = 2 and (etime is null or etime > ?)
	),
	counts as (
		select len from rkey
		where id = (select id from curkey)
	),
	bounds as (
		select
			case when ? < 0
				then (select len from counts) + ?
				else ?
			end as start,
			case when ? < 0
				then (select len from counts) + ?
				else ?
			end as stop
	),
	remain as (
		select ctid::text as rowid from rlist
		where kid = (select id from curkey)
		order by pos
		limit ((select stop from bounds) - (select start from bounds) + 1)
		offset (select start from bounds)
	)
	delete from rlist
	where
		kid = (select id from curkey)
		and ctid::text not in (select rowid from remain)`
)

// Tx is a list repository transaction.
type Tx struct {
	tx sqlx.Tx
}

// NewTx creates a list repository transaction
// from a generic database transaction.
func NewTx(tx sqlx.Tx) *Tx {
	return &Tx{tx}
}

// Delete deletes all occurrences of an element from a list.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) Delete(key string, elem any) (int, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	args := []any{key, time.Now().UnixMilli(), elemb}
	res, err := tx.tx.Exec(sqlDelete, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// DeleteBack deletes the first count occurrences of an element
// from a list, starting from the back. Count must be positive.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) DeleteBack(key string, elem any, count int) (int, error) {
	return tx.delete(key, elem, count, sqlDeleteBack)
}

// DeleteFront deletes the first count occurrences of an element
// from a list, starting from the front. Count must be positive.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) DeleteFront(key string, elem any, count int) (int, error) {
	return tx.delete(key, elem, count, sqlDeleteFront)
}

// Get returns an element from a list by index (0-based).
// Negative index count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the index is out of bounds, returns ErrNotFound.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) Get(key string, idx int) (core.Value, error) {
	var query = sqlGet
	if idx < 0 {
		// Reverse the query ordering and index, e.g.:
		//  - [11 12 13 14], idx = -1 <-> [14 13 12 11], idx = 0 (14)
		//  - [11 12 13 14], idx = -2 <-> [14 13 12 11], idx = 1 (13)
		//  - [11 12 13 14], idx = -3 <-> [14 13 12 11], idx = 2 (12)
		//  - etc.
		query = strings.Replace(query, sqlx.Asc, sqlx.Desc, 1)
		idx = -idx - 1
	}

	var val []byte
	args := []any{key, time.Now().UnixMilli(), idx}
	err := tx.tx.QueryRow(query, args...).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return core.Value(val), nil
}

// InsertAfter inserts an element after another element (pivot).
// Returns the length of the list after the operation.
// If the pivot does not exist, returns (-1, ErrNotFound).
// If the key does not exist or is not a list, returns (0, ErrNotFound).
func (tx *Tx) InsertAfter(key string, pivot, elem any) (int, error) {
	return tx.insert(key, pivot, elem, sqlInsertAfter)
}

// InsertBefore inserts an element before another element (pivot).
// Returns the length of the list after the operation.
// If the pivot does not exist, returns (-1, ErrNotFound).
// If the key does not exist or is not a list, returns (0, ErrNotFound).
func (tx *Tx) InsertBefore(key string, pivot, elem any) (int, error) {
	return tx.insert(key, pivot, elem, sqlInsertBefore)
}

// Len returns the number of elements in a list.
// If the key does not exist or is not a list, returns 0.
func (tx *Tx) Len(key string) (int, error) {
	var count int
	args := []any{key, time.Now().UnixMilli()}
	query := sqlx.AdaptPostgresQuery(sqlx.ConvertPlaceholders(sqlLen))
	err := tx.tx.QueryRow(query, args...).Scan(&count)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, sqlx.TypedError(err)
	}
	return count, nil
}

// PopBack removes and returns the last element of a list.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) PopBack(key string) (core.Value, error) {
	return tx.pop(key, sqlPopBack)
}

// PopBackPushFront removes the last element of a list
// and prepends it to another list (or the same list).
// If the source key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) PopBackPushFront(src, dest string) (core.Value, error) {
	// Pop the last element from the source list.
	elem, err := tx.PopBack(src)
	if err == core.ErrNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Prepend the element to the destination list.
	_, err = tx.PushFront(dest, elem.Bytes())
	return elem, err
}

// PopFront removes and returns the first element of a list.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) PopFront(key string) (core.Value, error) {
	return tx.pop(key, sqlPopFront)
}

// PushBack appends an element to a list.
// Returns the length of the list after the operation.
// If the key does not exist, creates it.
// If the key exists but is not a list, returns ErrKeyType.
func (tx *Tx) PushBack(key string, elem any) (int, error) {
	return tx.push(key, elem, sqlPushBack)
}

// PushFront prepends an element to a list.
// Returns the length of the list after the operation.
// If the key does not exist, creates it.
// If the key exists but is not a list, returns ErrKeyType.
func (tx *Tx) PushFront(key string, elem any) (int, error) {
	return tx.push(key, elem, sqlPushFront)
}

// Range returns a range of elements from a list.
// Both start and stop are zero-based, inclusive.
// Negative indexes count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the key does not exist or is not a list, returns an empty slice.
func (tx *Tx) Range(key string, start, stop int) ([]core.Value, error) {
	if (start > stop) && (start > 0 && stop > 0 || start < 0 && stop < 0) {
		return nil, nil
	}

	args := []any{
		key, time.Now().UnixMilli(),
		start, start, start,
		stop, stop, stop,
	}

	// For PostgreSQL, we need to use a different query with LIMIT/OFFSET syntax
	// instead of the SQLite LIMIT x,y syntax
	query := `
	with curkey as (
		select id from rkey
		where key = $1 and type = 2 and (etime is null or etime > $2)
	),
	counts as (
		select len from rkey
		where id = (select id from curkey)
	),
	bounds as (
		select
			case when $3 < 0
				then (select len from counts) + $4
				else $5
			end as start,
			case when $6 < 0
				then (select len from counts) + $7
				else $8
			end as stop
	)
	select elem
	from rlist
	where kid = (select id from curkey)
	order by pos
	limit ((select stop from bounds) - (select start from bounds) + 1)
	offset (select start from bounds)`

	rows, err := tx.tx.Query(query, args...)
	if err != nil {
		return nil, sqlx.TypedError(err)
	}
	defer rows.Close()

	var values []core.Value
	for rows.Next() {
		var val []byte
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}
		values = append(values, core.Value(val))
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

// Set sets an element in a list by index (0-based).
// Negative index count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the index is out of bounds, returns ErrNotFound.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) Set(key string, idx int, elem any) error {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return err
	}

	var query = sqlSet
	if idx < 0 {
		// Reverse the query ordering and index.
		query = strings.Replace(query, sqlx.Asc, sqlx.Desc, 1)
		idx = -idx - 1
	}

	args := []any{key, time.Now().UnixMilli(), elemb, idx}
	query = sqlx.AdaptPostgresQuery(sqlx.ConvertPlaceholders(query))
	out, err := tx.tx.Exec(query, args...)
	if err != nil {
		return sqlx.TypedError(err)
	}
	n, _ := out.RowsAffected()
	if n == 0 {
		return core.ErrNotFound
	}
	return err
}

// Trim removes elements from both ends of a list so that
// only the elements between start and stop indexes remain.
// Returns the number of elements removed.
//
// Both start and stop are zero-based, inclusive.
// Negative indexes count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
//
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) Trim(key string, start, stop int) (int, error) {
	args := []any{
		key, time.Now().UnixMilli(),
		start, start, start,
		stop, stop, stop,
	}

	var query string
	if sqlx.IsPostgres() {
		// PostgreSQL version with CTEs
		query = `
		with curkey as (
			select id from rkey
			where key = $1 and type = 2 and (etime is null or etime > $2)
		),
		counts as (
			select len from rkey
			where id = (select id from curkey)
		),
		bounds as (
			select
				case when $3 < 0
					then (select len from counts) + $4
					else $5
				end as start,
				case when $6 < 0
					then (select len from counts) + $7
					else $8
				end as stop
		),
		remain as (
			select ctid::text as rowid from rlist
			where kid = (select id from curkey)
			order by pos
			limit ((select stop from bounds) - (select start from bounds) + 1)
			offset (select start from bounds)
		)
		delete from rlist
		where
			kid = (select id from curkey)
			and ctid::text not in (select rowid from remain)`
	} else {
		// SQLite version
		query = `
		with curkey as (
			select id, len from rkey
			where key = ? and type = 2 and (etime is null or etime > ?)
		),
		bounds as (
			select
				case when ? < 0
					then (select len from curkey) + ?
					else ?
				end as start,
				case when ? < 0
					then (select len from curkey) + ?
					else ?
				end as stop
		),
		remain as (
			select rlist.rowid from rlist
			where kid = (select id from curkey)
			order by pos
			limit (select stop - start + 1 from bounds)
			offset (select start from bounds)
		)
		delete from rlist
		where
			kid = (select id from curkey)
			and rowid not in (select rowid from remain)`
	}

	out, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, sqlx.TypedError(err)
	}
	n, _ := out.RowsAffected()
	return int(n), nil
}

// delete deletes items from a list.
func (tx *Tx) delete(key string, elem any, count int, query string) (int, error) {
	// Convert the element value to bytes.
	elb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	// Delete elements from a list.
	now := time.Now().UnixMilli()
	var args []any
	if count > 0 {
		args = []any{key, now, elb, count}
	} else {
		args = []any{key, now, elb}
	}
	query = sqlx.ConvertPlaceholders(query)
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, sqlx.TypedError(err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// insert inserts an element into a list.
func (tx *Tx) insert(key string, pivot, elem any, query string) (int, error) {
	// Convert the pivot and element values to bytes.
	pivotb, err := core.ToBytes(pivot)
	if err != nil {
		return 0, err
	}
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	// Update the key.
	now := time.Now().UnixMilli()
	sqlUpdateQuery := sqlx.ConvertPlaceholders(sqlInsert)
	args := []any{now, key, now}
	var keyID, n int
	err = tx.tx.QueryRow(sqlUpdateQuery, args...).Scan(&keyID, &n)
	if err == sql.ErrNoRows {
		return 0, core.ErrNotFound
	}
	if err != nil {
		return 0, sqlx.TypedError(err)
	}

	// Insert the element.
	query = sqlx.ConvertPlaceholders(query)
	args = []any{keyID, pivotb, keyID, keyID, elemb, keyID}
	_, err = tx.tx.Exec(query, args...)
	if err != nil {
		if sqlx.ConstraintFailed(err, "NOT NULL", "rlist.pos") {
			return -1, core.ErrNotFound
		}
		return 0, sqlx.TypedError(err)
	}

	return n, nil
}

// pop removes and returns the first/last element from a list.
func (tx *Tx) pop(key string, query string) (core.Value, error) {
	// Get the first/last element from a list.
	now := time.Now().UnixMilli()
	args := []any{key, now}
	query = sqlx.ConvertPlaceholders(query)

	// For PostgreSQL, we need to modify the query to only return the element
	if sqlx.IsPostgres() {
		// Modify the query to only return the element
		if strings.Contains(query, "returning elem") {
			query = strings.Replace(query, "returning elem", "returning elem, kid", 1)
		}

		var elem []byte
		var kid int
		err := tx.tx.QueryRow(query, args...).Scan(&elem, &kid)
		if err == sql.ErrNoRows {
			return core.Value(nil), core.ErrNotFound
		}
		if err != nil {
			return core.Value(nil), sqlx.TypedError(err)
		}
		return core.Value(elem), nil
	} else {
		// SQLite version
		var elem []byte
		err := tx.tx.QueryRow(query, args...).Scan(&elem)
		if err == sql.ErrNoRows {
			return core.Value(nil), core.ErrNotFound
		}
		if err != nil {
			return core.Value(nil), sqlx.TypedError(err)
		}
		return core.Value(elem), nil
	}
}

// push appends an element to a list.
func (tx *Tx) push(key string, elem any, query string) (int, error) {
	// Convert the element value to bytes
	elb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	// First create or update the key
	now := time.Now().UnixMilli()
	var keyID, length int
	sqlInsertQuery := sqlx.AdaptPostgresQuery(sqlx.ConvertPlaceholders(sqlPush))
	err = tx.tx.QueryRow(sqlInsertQuery, key, now).Scan(&keyID, &length)
	if err != nil {
		return 0, sqlx.TypedError(err)
	}

	// Now insert the element - we need to match the exact params for each query
	query = sqlx.AdaptPostgresQuery(sqlx.ConvertPlaceholders(query))
	var args []any

	// All our push queries need kid, elem, kid pattern
	args = []any{keyID, elb, keyID}

	_, err = tx.tx.Exec(query, args...)
	if err != nil {
		return 0, sqlx.TypedError(err)
	}

	return length, nil
}
