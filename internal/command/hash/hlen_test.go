package hash

import (
	"testing"

	"github.com/flarco/redka/internal/redis"
	"github.com/flarco/redka/internal/testx"
)

func TestHLenParse(t *testing.T) {
	tests := []struct {
		cmd string
		key string
		err error
	}{
		{
			cmd: "hlen",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "hlen person",
			key: "person",
			err: nil,
		},
		{
			cmd: "hlen person name",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHLen, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.key)
			} else {
				testx.AssertEqual(t, cmd, HLen{})
			}
		})
	}
}

func TestHLenExec(t *testing.T) {
	t.Run("key found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHLen, "hlen person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseHLen, "hlen person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
