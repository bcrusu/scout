package storage

import (
	"fmt"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
)

var (
	_ DB = (*dbx)(nil)
)

// DB represents the possible features a backing storage implementation can provide.
// At a minimum, the key-value feature must be provided, while the MVCC feature will
// be emulated if not available, albeit in an less-performant way that involves more
// KV records fetched to memory.
type DB interface {
	KV() kv.DB
	MVCC() mvcc.DB
}

type dbx struct {
	kv   kv.DB
	mvcc mvcc.DB
}

func NewDB(impl any) DB {
	if x, ok := impl.(DB); ok {
		return x
	}

	if x, ok := impl.(kv.DB); ok {
		return &dbx{
			kv:   x,
			mvcc: mvcc.NewEmulated(x),
		}
	}

	panic(fmt.Sprintf("Could not create DB for param %T", impl))
}

func (p *dbx) KV() kv.DB {
	return p.kv
}

func (p *dbx) MVCC() mvcc.DB {
	return p.mvcc
}
