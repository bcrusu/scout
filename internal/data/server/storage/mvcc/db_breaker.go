package mvcc

import (
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/utils"
)

// DBBreaker ensures operation success or total destruction.
type DBBreaker struct {
	db          DB
	retryPolicy utils.RetryPolicy
}

func NewDBBreaker(db DB, retryPolicy utils.RetryPolicy) *DBBreaker {
	return &DBBreaker{
		db:          db,
		retryPolicy: retryPolicy,
	}
}

func (d *DBBreaker) Get(addr kv.Address) *kv.Entry {
	value, err := utils.RetryR(d.retryPolicy, func() (*kv.Entry, error) {
		return d.db.Get(addr)
	})

	if err != nil {
		utils.ShutdownNow("DBBreaker.Get failed")
	}

	return value
}

func (d *DBBreaker) GetRange(rang Range) kv.Iterator {
	iter, err := utils.RetryR(d.retryPolicy, func() (kv.Iterator, error) {
		return d.db.GetRange(rang)
	})

	if err != nil {
		utils.ShutdownNow("DBBreaker.GetRange failed")
	}

	return iter
}

func (d *DBBreaker) Exists(addr kv.Address) bool {
	result, err := utils.RetryR(d.retryPolicy, func() (bool, error) {
		return d.db.Exists(addr)
	})

	if err != nil {
		utils.ShutdownNow("DBBreaker.Exists failed")
	}

	return result
}

func (d *DBBreaker) ExistsInRange(rang Range) bool {
	result, err := utils.RetryR(d.retryPolicy, func() (bool, error) {
		return d.db.ExistsInRange(rang)
	})

	if err != nil {
		utils.ShutdownNow("DBBreaker.ExistsInRange failed")
	}

	return result
}

func (d *DBBreaker) Put(index uint64, entries ...kv.Entry) {
	err := utils.RetryE(d.retryPolicy, func() error {
		return d.db.Put(index, entries...)
	})

	if err != nil {
		utils.ShutdownNow("DBBreaker.Set failed")
	}
}
