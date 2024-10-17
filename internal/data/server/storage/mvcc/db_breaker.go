package mvcc

import (
	"github.com/bcrusu/scout/internal/data/server/config"
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

func (d *DBBreaker) Get(pid uint32, timestamp uint64, addrs ...Address) []*Record {
	value, err := utils.RetryR(d.retryPolicy, func() ([]*Record, error) {
		return d.db.Get(pid, timestamp, addrs...)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.Get failed with error=%s", err)
	}

	return value
}

func (d *DBBreaker) GetRange(pid uint32, timestamp uint64, start, end Address) Iterator {
	iter, err := utils.RetryR(d.retryPolicy, func() (Iterator, error) {
		return d.db.GetRange(pid, timestamp, start, end)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.GetRange failed with error=%s", err)
	}

	return iter
}

func (d *DBBreaker) Exists(pid uint32, timestamp uint64, addr Address) bool {
	result, err := utils.RetryR(d.retryPolicy, func() (bool, error) {
		return d.db.Exists(pid, timestamp, addr)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.Exists failed with error=%s", err)
	}

	return result
}

func (d *DBBreaker) ExistsInRange(pid uint32, timestamp uint64, start, end Address) bool {
	result, err := utils.RetryR(d.retryPolicy, func() (bool, error) {
		return d.db.ExistsInRange(pid, timestamp, start, end)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.ExistsInRange failed with error=%s", err)
	}

	return result
}

func (d *DBBreaker) Put(pid uint32, index uint64, records ...Record) {
	err := utils.RetryE(d.retryPolicy, func() error {
		return d.db.Put(pid, index, records...)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.Put failed with error=%s", err)
	}
}
