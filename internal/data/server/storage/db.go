package storage

import (
	"time"

	"github.com/bcrusu/graph/internal/utils"
)

// TODO: make configurable
var (
	maxRetries   = 10
	retryBackoff = &utils.Backoff{
		MinDelay: 50 * time.Millisecond,
		MaxDelay: 100 * time.Millisecond,
	}
)

type Location struct {
	PartitionID uint32
	Keyspace    uint64
	Key         []byte
	Version     uint64 // optional; when not specified, it represents latest value
}

type Range struct {
	PartitionID uint32
	Keyspace    uint64
	StartKey    []byte // inclusive
	EndKey      []byte // exclusive
	Version     uint64 // optional; when not specified, it represents latest value
}

type Iterator interface {
	//TODO
}

type DB interface {
	Get(Location) ([]byte, error)
	GetRange(Range) (Iterator, error)
	Exists(Location) (bool, error)
	ExistsInRange(Range) (bool, error)
	Set(Location, []byte) error
	Delete(Location) error
}

// crticalDB wraps the DB object to ensure operation success or total destruction.
type crticalDB struct {
	db DB
}

type crticalIterator struct {
	inner Iterator
}

func (d *crticalDB) Get(loc Location) []byte {
	result, err := utils.RetryR(maxRetries, retryBackoff, func() ([]byte, error) {
		return d.db.Get(loc)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.Get failed")
	}

	return result
}

func (d *crticalDB) GetRange(rang Range) Iterator {
	iter, err := utils.RetryR(maxRetries, retryBackoff, func() (Iterator, error) {
		return d.db.GetRange(rang)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.GetRange failed")
	}

	return &crticalIterator{
		inner: iter,
	}
}

func (d *crticalDB) Exists(loc Location) bool {
	result, err := utils.RetryR(maxRetries, retryBackoff, func() (bool, error) {
		return d.db.Exists(loc)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.Exists failed")
	}

	return result
}

func (d *crticalDB) ExistsInRange(rang Range) bool {
	result, err := utils.RetryR(maxRetries, retryBackoff, func() (bool, error) {
		return d.db.ExistsInRange(rang)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.ExistsInRange failed")
	}

	return result
}

func (d *crticalDB) Set(loc Location, value []byte) {
	err := utils.RetryE(maxRetries, retryBackoff, func() error {
		return d.db.Set(loc, value)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.Set failed")
	}
}

func (d *crticalDB) Delete(loc Location) {
	err := utils.RetryE(maxRetries, retryBackoff, func() error {
		return d.db.Delete(loc)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.Delete failed")
	}
}
