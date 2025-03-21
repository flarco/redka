package hash

import (
	"testing"

	"github.com/flarco/redka/internal/redis"
	"github.com/flarco/redka/internal/testx"
)

func TestHSetNXParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want HSetNX
		err  error
	}{
		{
			cmd:  "hsetnx",
			want: HSetNX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "hsetnx person",
			want: HSetNX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "hsetnx person name",
			want: HSetNX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "hsetnx person name alice",
			want: HSetNX{key: "person", field: "name", value: []byte("alice")},
			err:  nil,
		},
		{
			cmd:  "hsetnx person name alice age 25",
			want: HSetNX{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHSetNX, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.value, test.want.value)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestHSetNXExec(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseHSetNX, "hsetnx person name alice")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "1")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := redis.MustParse(ParseHSetNX, "hsetnx person name bob")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.Out(), "0")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
	})
}
