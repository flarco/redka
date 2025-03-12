package string

import (
	"testing"

	"github.com/flarco/redka/internal/core"
	"github.com/flarco/redka/internal/redis"
	"github.com/flarco/redka/internal/testx"
)

func TestGetParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want string
		err  error
	}{
		{
			cmd:  "get",
			want: "",
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "get name",
			want: "name",
			err:  nil,
		},
		{
			cmd:  "get name age",
			want: "",
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseGet, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want)
			} else {
				testx.AssertEqual(t, cmd, Get{})
			}
		})
	}
}

func TestGetExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	_ = db.Str().Set("name", "alice")

	tests := []struct {
		cmd string
		res any
		out string
	}{
		{
			cmd: "get name",
			res: core.Value("alice"),
			out: "alice",
		},
		{
			cmd: "get age",
			res: core.Value(nil),
			out: "(nil)",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseGet, test.cmd)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.Out(), test.out)
		})
	}
}
