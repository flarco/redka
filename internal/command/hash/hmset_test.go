package hash

import (
	"testing"

	"github.com/flarco/redka/internal/redis"
	"github.com/flarco/redka/internal/testx"
)

func TestHMSetParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want HMSet
		err  error
	}{
		{
			cmd:  "hmset",
			want: HMSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "hmset person",
			want: HMSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "hmset person name",
			want: HMSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "hmset person name alice",
			want: HMSet{key: "person", items: map[string]any{"name": []byte("alice")}},
			err:  nil,
		},
		{
			cmd:  "hmset person name alice age",
			want: HMSet{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd: "hmset person name alice age 25",
			want: HMSet{key: "person", items: map[string]any{
				"name": []byte("alice"),
				"age":  []byte("25"),
			}},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHMSet, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.items, test.want.items)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestHMSetExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseHMSet, "hmset person name alice")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "OK")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("create multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseHMSet, "hmset person name alice age 25")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "OK")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "25")
	})

	t.Run("create/update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := redis.MustParse(ParseHMSet, "hmset person name bob age 50")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "OK")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "bob")
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "50")
	})

	t.Run("update multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHMSet, "hmset person name bob age 50")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "OK")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "bob")
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "50")
	})
}
