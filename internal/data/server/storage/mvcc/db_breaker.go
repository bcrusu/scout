package mvcc

import (
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/utils"
)

// DBBreaker ensures operation success or total destruction.
type DBBreaker struct {
	db          DB
	retryPolicy utils.RetryPolicy
}

func NewDBBreaker(db DB) *DBBreaker {
	return &DBBreaker{
		db:          db,
		retryPolicy: config.Get().DB.RetryPolicy,
	}
}

func (d *DBBreaker) Get(addr kv.Address) *Entry {
	value, err := utils.RetryR(d.retryPolicy, func() (*Entry, error) {
		return d.db.Get(addr)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.Get failed with error=%s", err)
	}

	return value
}

func (d *DBBreaker) GetRange(rang Range) Iterator {
	iter, err := utils.RetryR(d.retryPolicy, func() (Iterator, error) {
		return d.db.GetRange(rang)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.GetRange failed with error=%s", err)
	}

	return iter
}

func (d *DBBreaker) Exists(addr kv.Address) bool {
	result, err := utils.RetryR(d.retryPolicy, func() (bool, error) {
		return d.db.Exists(addr)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.Exists failed with error=%s", err)
	}

	return result
}

func (d *DBBreaker) ExistsInRange(rang Range) bool {
	result, err := utils.RetryR(d.retryPolicy, func() (bool, error) {
		return d.db.ExistsInRange(rang)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.ExistsInRange failed with error=%s", err)
	}

	return result
}

func (d *DBBreaker) Put(index uint64, entries ...Entry) {
	err := utils.RetryE(d.retryPolicy, func() error {
		return d.db.Put(index, entries...)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.Put failed with error=%s", err)
	}
}
