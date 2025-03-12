package set

import (
	"testing"

	"github.com/flarco/redka/internal/core"
	"github.com/flarco/redka/internal/redis"
	"github.com/flarco/redka/internal/testx"
)

func TestSDiffParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SDiff
		err  error
	}{
		{
			cmd:  "sdiff",
			want: SDiff{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sdiff key",
			want: SDiff{keys: []string{"key"}},
			err:  nil,
		},
		{
			cmd:  "sdiff k1 k2",
			want: SDiff{keys: []string{"k1", "k2"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSDiff, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.keys, test.want.keys)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestSDiffExec(t *testing.T) {
	t.Run("non-empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one", "two", "thr", "fiv")
		_, _ = db.Set().Add("key2", "two", "fou", "six")
		_, _ = db.Set().Add("key3", "thr", "six")

		cmd := redis.MustParse(ParseSDiff, "sdiff key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 2)
		testx.AssertEqual(t, conn.Out(), "2,fiv,one")
	})
	t.Run("no keys", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseSDiff, "sdiff key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("single key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one", "two", "thr")

		cmd := redis.MustParse(ParseSDiff, "sdiff key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 3)
		testx.AssertEqual(t, conn.Out(), "3,one,thr,two")
	})
	t.Run("empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one", "two")
		_, _ = db.Set().Add("key2", "one", "fou")
		_, _ = db.Set().Add("key3", "two", "fiv")

		cmd := redis.MustParse(ParseSDiff, "sdiff key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("first not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key2", "two")
		_, _ = db.Set().Add("key3", "thr")

		cmd := redis.MustParse(ParseSDiff, "sdiff key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("rest not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one")
		_, _ = db.Set().Add("key2", "two")

		cmd := redis.MustParse(ParseSDiff, "sdiff key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 1)
		testx.AssertEqual(t, conn.Out(), "1,one")
	})
	t.Run("all not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseSDiff, "sdiff key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one")
		_ = db.Str().Set("key2", "two")
		_, _ = db.Set().Add("key3", "thr")

		cmd := redis.MustParse(ParseSDiff, "sdiff key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 1)
		testx.AssertEqual(t, conn.Out(), "1,one")
	})
}
