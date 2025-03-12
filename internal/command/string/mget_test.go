package string

import (
	"testing"

	"github.com/flarco/redka/internal/core"
	"github.com/flarco/redka/internal/redis"
	"github.com/flarco/redka/internal/testx"
)

func TestMGetParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want []string
		err  error
	}{
		{
			cmd:  "mget",
			want: nil,
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "mget name",
			want: []string{"name"},
			err:  nil,
		},
		{
			cmd:  "mget name age",
			want: []string{"name", "age"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseMGet, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.keys, test.want)
			} else {
				testx.AssertEqual(t, cmd, MGet{})
			}
		})
	}
}

func TestMGetExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	tests := []struct {
		cmd string
		res any
		out string
	}{
		{
			cmd: "mget name",
			res: []core.Value{core.Value("alice")},
			out: "1,alice",
		},
		{
			cmd: "mget name age",
			res: []core.Value{core.Value("alice"), core.Value("25")},
			out: "2,alice,25",
		},
		{
			cmd: "mget name city age",
			res: []core.Value{core.Value("alice"), core.Value(nil), core.Value("25")},
			out: "3,alice,(nil),25",
		},
		{
			cmd: "mget one two",
			res: []core.Value{core.Value(nil), core.Value(nil)},
			out: "2,(nil),(nil)",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseMGet, test.cmd)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.Out(), test.out)
		})
	}
}
