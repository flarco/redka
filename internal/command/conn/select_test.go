package conn

import (
	"testing"

	"github.com/flarco/redka/internal/redis"
	"github.com/flarco/redka/internal/testx"
)

func TestSelectParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want Select
		err  error
	}{
		{
			cmd:  "select",
			want: Select{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "select 5",
			want: Select{index: 5},
			err:  nil,
		},
		{
			cmd:  "select five",
			want: Select{},
			err:  redis.ErrInvalidInt,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSelect, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.index, test.want.index)
			} else {
				testx.AssertEqual(t, cmd, Select{})
			}
		})
	}
}

func TestSelectExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	tests := []struct {
		cmd string
		res any
		out string
	}{
		{
			cmd: "select 5",
			res: true,
			out: "OK",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseSelect, test.cmd)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.Out(), test.out)
		})
	}
}
