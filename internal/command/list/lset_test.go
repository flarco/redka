package list

import (
	"testing"

	"github.com/flarco/redka/internal/core"
	"github.com/flarco/redka/internal/redis"
	"github.com/flarco/redka/internal/testx"
)

func TestLSetParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want LSet
		err  error
	}{
		{
			cmd:  "lset",
			want: LSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lset key",
			want: LSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lset key elem",
			want: LSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "lset key elem 5",
			want: LSet{},
			err:  redis.ErrInvalidInt,
		},
		{
			cmd:  "lset key 5 elem",
			want: LSet{key: "key", index: 5, elem: []byte("elem")},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseLSet, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.index, test.want.index)
				testx.AssertEqual(t, cmd.elem, test.want.elem)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestLSetExec(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseLSet, "lset key 0 elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertErr(t, err, core.ErrNotFound)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), redis.ErrOutOfRange.Error()+" (lset)")
	})
	t.Run("set elem", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLSet, "lset key 1 upd")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), "OK")

		el1, _ := db.List().Get("key", 1)
		testx.AssertEqual(t, el1.String(), "upd")
	})
	t.Run("negative index", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")
		_, _ = db.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLSet, "lset key -1 upd")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), "OK")

		el2, _ := db.List().Get("key", 2)
		testx.AssertEqual(t, el2.String(), "upd")
	})
	t.Run("index out of bounds", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "elem")

		cmd := redis.MustParse(ParseLSet, "lset key 1 upd")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertErr(t, err, core.ErrNotFound)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), redis.ErrOutOfRange.Error()+" (lset)")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		cmd := redis.MustParse(ParseLSet, "lset key 0 elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertErr(t, err, core.ErrNotFound)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), redis.ErrOutOfRange.Error()+" (lset)")
	})
}
