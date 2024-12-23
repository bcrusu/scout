package hlc

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	logicalMask  = uint64(1<<16) - 1
	logicalMax   = logicalMask
	physicalMask = math.MaxUint - logicalMax
	physicalMin  = time.Duration(logicalMax + 1)
	maxRetries   = 3
)

var (
	global *Hlc
	log    = logging.New("hlc")
)

// Hlc implements the Hybrid Logical Clock as described in paper: "Logical Physical Clocks
// and Consistent Snapshots in Globally Distributed Databases" by Sandeep Kulkarni*, Murat
// Demirbas**, Deepak Madeppa**, Bharadwaj Avva**, and Marcelo Leone* (* Michigan State
// University, ** University at Buffalo, SUNY)
type Hlc struct {
	maxOffset   uint64
	lock        sync.Mutex
	physical    uint64
	logical     uint64
	statsNow    StatsNow
	statsUpdate StatsUpdate
}

type StatsNow struct {
	Total         int
	LogicalReset  int
	LogicalInc    int
	BackwardJumps int
	HitLogicalMax int
}

type StatsUpdate struct {
	Total         int
	OutOfRange    int
	LogicalReset  int
	LogicalTies   int
	LogicalOurs   int
	LogicalTheirs int
	HitLogicalMax int
}

// Get returns the global Hlc instance.
func Get() *Hlc {
	if global == nil {
		panic("HLC was not set")
	}
	return global
}

// Set sets the global Hlc instance.
func Set(hlc *Hlc) {
	if global != nil {
		panic("HLC already set")
	}
	global = hlc
}

// New returns a new Hlc instance.
func New(maxOffset time.Duration) *Hlc {
	return &Hlc{
		maxOffset: physical(uint64(maxOffset)),
		physical:  physicalNow(),
	}
}

// Now returns the current HLC timestamp.
func (h *Hlc) Now() uint64 {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.statsNow.Total++

	for range maxRetries {
		now := physicalNow()

		if now > h.physical {
			h.statsNow.LogicalReset++
			h.physical = now
			h.logical = 0
			return now
		} else if now < h.physical {
			h.statsNow.BackwardJumps++
			diff := h.physical - now
			log.Warn("Wall clock backward jump detected.", "current", now, "previous", h.physical, "diff", diff)

			if diff > h.maxOffset {
				utils.ShutdownNow("Wall clock jumped backward more than allowed.")
			}

			time.Sleep(time.Duration(diff))
			continue
		}

		if h.logical == logicalMax {
			// For this execution path to happen it would require 2^16 Now calls inside the
			// physicalMin duration of 65.537µs (with 16 bits logical counter). Did not even
			// come close to this value during benchmarks, but leaving the check for completeness.
			h.statsNow.HitLogicalMax++
			time.Sleep(physicalMin / 2)
			continue
		}

		h.statsNow.LogicalInc++
		h.logical++
		return h.physical | h.logical
	}

	utils.ShutdownNow("HLC.Now failed too many times")
	panic("unreachable")
}

// Update sets the current HLC timestamp using the incoming timestamp.
// It returns TimeOutOfRange error if the value is too far in the future.
func (h *Hlc) Update(incoming uint64) error {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.statsUpdate.Total++

	inPhysical, inLogical := split(incoming)
	if err := h.checkUpdateTimeOffset(inPhysical); err != nil {
		return err
	}

	for range maxRetries {
		nextPhysical := max(h.physical, inPhysical, physicalNow())
		nextLogical := uint64(0)

		switch {
		case nextPhysical == h.physical && nextPhysical == inPhysical:
			h.statsUpdate.LogicalTies++
			nextLogical = max(h.logical, inLogical) + 1
		case nextPhysical == h.physical:
			h.statsUpdate.LogicalOurs++
			nextLogical = h.logical + 1
		case nextPhysical == inPhysical:
			h.statsUpdate.LogicalTheirs++
			nextLogical = inLogical + 1
		default:
			h.statsUpdate.LogicalReset++
		}

		if nextLogical > logicalMax {
			// Similar to above, this execution path is very unlikely.
			h.statsUpdate.HitLogicalMax++
			time.Sleep(physicalMin / 2)
			continue
		}

		h.physical = nextPhysical
		h.logical = nextLogical
		return nil
	}

	utils.ShutdownNow("HLC.Update failed too many times")
	panic("unreachable")
}

func (h *Hlc) Stats() (StatsNow, StatsUpdate) {
	return h.statsNow, h.statsUpdate
}

func (h *Hlc) checkUpdateTimeOffset(inPhysical uint64) error {
	now := physicalNow()
	if inPhysical <= now {
		return nil
	}

	diff := inPhysical - now

	if diff > h.maxOffset {
		h.statsUpdate.OutOfRange++
		return errors.TimeOutOfRange
	}

	return nil
}

// Now returns the current HLC timestamp.
func Now() uint64 {
	return Get().Now()
}

// Update sets the current HLC timestamp using the incoming timestamp.
// It returns TimeOutOfRange error if the value is too far in the future.
func Update(incoming uint64) error {
	return Get().Update(incoming)
}

// AsTime converts a HLC timestamp to the equivalent Time. It does not
// trim the logical HLC part to avoid data loss and make the operation
// reversible using FromTime method.
func AsTime(timestamp uint64) time.Time {
	x := int64(timestamp)
	sec := int64(x / 1e9)
	nsec := x % 1e9
	return time.Unix(sec, nsec).UTC()
}

// AsTimestamp converts a HLC timestamp to the equivalent Timestamp proto.
// It does not trim the logical HLC part to avoid data loss and make the
// operation reversible using FromTimestamp method.
func AsTimestamp(timestamp uint64) *timestamppb.Timestamp {
	return timestamppb.New(AsTime(timestamp))
}

// FromTime converts the Time to the equivalent HLC timestamp. It assumes
// that the value was previously constructed using AsTime and does not
// trim the logical part.
func FromTime(time time.Time) uint64 {
	return uint64(time.UnixNano())
}

// FromTimestamp converts a Timestamp proto to the equivalent HLC timestamp.
// It assumes that the proto was previously constructed using AsTimestamp and
// does not trim the logical part.
func FromTimestamp(ts *timestamppb.Timestamp) uint64 {
	return FromTime(ts.AsTime())
}

// Sleep returns when the deadline passed or the ctx is canceled.
func Sleep(ctx context.Context, deadline uint64) error {
	for {
		now := Now()
		if now > deadline {
			return nil
		}

		diff := Subtract(deadline, now)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(diff):
		}
	}
}

// Subtracts returns the physical time difference (a-b) between two timestamps.
func Subtract(a, b uint64) time.Duration {
	ap := physical(a)
	bp := physical(b)

	if ap >= bp {
		return time.Duration(ap - bp)
	}

	return -time.Duration(bp - ap)
}

func physicalNow() uint64 {
	return physical(uint64(time.Now().UnixNano()))
}

func physical(timestamp uint64) uint64 {
	return timestamp & physicalMask
}

func split(timestamp uint64) (uint64, uint64) {
	return timestamp & physicalMask, timestamp & logicalMask
}
