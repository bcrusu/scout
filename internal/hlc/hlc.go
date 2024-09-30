package hlc

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	global = New()
)

type Hlc struct {
}

func New() *Hlc {
	return &Hlc{}
}

// TODO: replace all time.Now calls with hlc.Now()
func (h *Hlc) Now() uint64 {
	return uint64(time.Now().UTC().UnixNano())
}

func (h *Hlc) Update(incoming uint64) {

}

func Now() uint64 {
	return global.Now()
}

func Update(incoming uint64) {
	global.Update(incoming)
}

func AsTime(timestamp uint64) time.Time {
	return time.Time{}
}

func AsTimestamp(timestamp uint64) *timestamppb.Timestamp {
	return nil
}

func FromTime(time time.Time) uint64 {
	return 0
}

func FromTimestamp(timestamp *timestamppb.Timestamp) uint64 {
	return 0
}
