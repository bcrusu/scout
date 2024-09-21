package utils

import (
	"fmt"

	"github.com/dustin/go-humanize"
)

type Bytes string

func (b Bytes) MustParse() uint32 {
	v, err := b.Parse()
	if err != nil {
		panic(fmt.Sprintf("failed to parse Bytes value %q", b))
	}
	return v
}

func (b Bytes) Parse() (uint32, error) {
	v, err := humanize.ParseBytes(string(b))
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}

// GetOptionalParameter is used by variadic functions to specify a single optional parameter.
func GetOptionalParameter[T any](defaultValue T, values []T) T {
	if len(values) == 1 {
		return values[0]
	} else if len(values) > 1 {
		panic("expected a single value")
	}
	return defaultValue
}
