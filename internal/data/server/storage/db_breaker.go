package storage

import (
	"github.com/bcrusu/graph/internal/utils"
)

// dbBreaker wraps the DB object to ensure operation success or total destruction.
type dbBreaker struct {
	db          DB
	retryPolicy utils.RetryPolicy
}

func (d *dbBreaker) Get(loc Location) *ValueAt {
	value, err := utils.RetryR(d.retryPolicy, func() (*ValueAt, error) {
		return d.db.Get(loc)
	})

	if err != nil {
		utils.ShutdownNow("dbBreaker.Get failed")
	}

	return value
}

func (d *dbBreaker) GetRange(rang Range) Iterator {
	iter, err := utils.RetryR(d.retryPolicy, func() (Iterator, error) {
		return d.db.GetRange(rang)
	})

	if err != nil {
		utils.ShutdownNow("dbBreaker.GetRange failed")
	}

	return iter
}

func (d *dbBreaker) Exists(loc Location) bool {
	result, err := utils.RetryR(d.retryPolicy, func() (bool, error) {
		return d.db.Exists(loc)
	})

	if err != nil {
		utils.ShutdownNow("dbBreaker.Exists failed")
	}

	return result
}

func (d *dbBreaker) ExistsInRange(rang Range) bool {
	result, err := utils.RetryR(d.retryPolicy, func() (bool, error) {
		return d.db.ExistsInRange(rang)
	})

	if err != nil {
		utils.ShutdownNow("dbBreaker.ExistsInRange failed")
	}

	return result
}

func (d *dbBreaker) Set(loc Location, data []byte) {
	err := utils.RetryE(d.retryPolicy, func() error {
		return d.db.Set(loc, data)
	})

	if err != nil {
		utils.ShutdownNow("dbBreaker.Set failed")
	}
}

func (d *dbBreaker) Delete(loc Location) {
	err := utils.RetryE(d.retryPolicy, func() error {
		return d.db.Delete(loc)
	})

	if err != nil {
		utils.ShutdownNow("dbBreaker.Delete failed")
	}
}
