package utils

import (
	"fmt"

	"github.com/dustin/go-humanize"
)

type Bytes string

func (b Bytes) MustParse() uint64 {
	v, err := b.Parse()
	if err != nil {
		panic(fmt.Sprintf("failed to parse Bytes value %q", b))
	}
	return v
}

func (b Bytes) Parse() (uint64, error) {
	v, err := humanize.ParseBytes(string(b))
	if err != nil {
		return 0, err
	}
	return uint64(v), nil
}
