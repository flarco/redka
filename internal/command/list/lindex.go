package list

import (
	"github.com/flarco/redka/internal/core"
	"github.com/flarco/redka/internal/parser"
	"github.com/flarco/redka/internal/redis"
)

// Returns an element from a list by its index.
// LINDEX key index
// https://redis.io/commands/lindex
type LIndex struct {
	redis.BaseCmd
	key   string
	index int
}

func ParseLIndex(b redis.BaseCmd) (LIndex, error) {
	cmd := LIndex{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.index),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return LIndex{}, err
	}
	return cmd, nil
}

func (cmd LIndex) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.List().Get(cmd.key, cmd.index)
	if err == core.ErrNotFound {
		w.WriteNull()
		return val, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulk(val)
	return val, nil
}
