package kv

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

func (d *DBBreaker) InitPartition(pid uint32) {
	err := utils.RetryE(d.retryPolicy, func() error {
		return d.db.InitPartition(pid)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.InitPartition %d failed with error %s", pid, err)
	}
}

func (d *DBBreaker) DropPartition(pid uint32) {
	err := utils.RetryE(d.retryPolicy, func() error {
		return d.db.DropPartition(pid)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.DropPartition %d failed with error %s", pid, err)
	}
}

func (d *DBBreaker) SyncPartition(pid uint32) {
	err := utils.RetryE(d.retryPolicy, func() error {
		return d.db.SyncPartition(pid)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.SyncPartition %d failed with error %s", pid, err)
	}
}

func (d *DBBreaker) GetIndex(pid uint32, persistedOnDisk bool) uint64 {
	index, err := utils.RetryR(d.retryPolicy, func() (uint64, error) {
		return d.db.GetIndex(pid, persistedOnDisk)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.GetIndex in partition %d failed with error %s", pid, err)
	}

	return index
}

func (d *DBBreaker) Put(pid uint32, index uint64, records ...Record) {
	err := utils.RetryE(d.retryPolicy, func() error {
		return d.db.Put(pid, index, records...)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.Put in partition %d failed with error %s", pid, err)
	}
}

func (d *DBBreaker) Get(pid uint32, address Address) *Record {
	record, err := utils.RetryR(d.retryPolicy, func() (*Record, error) {
		return d.db.Get(pid, address)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.Get in partition %d failed with error %s", pid, err)
	}

	return record
}

func (d *DBBreaker) GetFrom(pid uint32, start Address, end *Address) Iterator {
	iter, err := utils.RetryR(d.retryPolicy, func() (Iterator, error) {
		return d.db.GetRange(pid, start, end)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.GetFrom in partition %d failed with error %s", pid, err)
	}

	return iter
}

func (d *DBBreaker) GetStream(pid uint32, start Address) Iterator {
	iter, err := utils.RetryR(d.retryPolicy, func() (Iterator, error) {
		return d.db.GetStream(pid, start)
	})

	if err != nil {
		utils.ShutdownNowf("DBBreaker.GetStream in partition %d failed with error %s", pid, err)
	}

	return iter
}
