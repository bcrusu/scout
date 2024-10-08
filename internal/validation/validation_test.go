package validation_test

import (
	"strings"
	"testing"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/internal/utils/tests"
	"github.com/bcrusu/scout/internal/validation"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUtils(t *testing.T) {
	tests.NewSuite(t, "validation test suite")
}

type WithCanValidate struct {
	Value   string `validate:"required"`
	Nested  WithCanValidateNested
	Nested2 WithCanValidateNested `validate:"skip"`
}

type WithCanValidateNested struct {
	Value string `validate:"minLen:100"`
}

func (v WithCanValidate) Validate() error {
	if v.Value == "invalid" {
		return errors.Error("root is invalid")
	}
	return nil
}

func (v WithCanValidateNested) Validate() error {
	if v.Value == "invalid" {
		return errors.Error("nested is invalid")
	}
	return nil
}

var _ = Describe("Validation tests", func() {
	type Nested1 struct {
		Int      int           `validate:"max:-100"`
		Duration time.Duration `validate:"min:100ms"`
	}
	type Nested2 struct {
		Float64 float64 `validate:"min:10"`
		Uint    float64 `validate:"max:1000"`
		Nested1 Nested1 `validate:"required"`
	}
	type Nested3 struct {
		Duration time.Duration `validate:"max:1s"`
		Uint16   uint16        `validate:"required"`
		Nested1  Nested1       `validate:"required"`
		Nested2  *Nested2
		Nested22 *Nested2 `validate:"required"`
	}
	type Validate struct {
		Int       int           `validate:"min:-100,max:100,required"`
		Int8      int8          `validate:"min:-100,max:100,required"`
		Int16     int16         `validate:"min:-100,max:100,required"`
		Int32     int32         `validate:"min:-100,max:100,required"`
		Int64     int32         `validate:"min:-100,max:100,required"`
		UInt      uint          `validate:"min:10,max:20"`
		UInt8     uint8         `validate:"min:10,max:20"`
		UInt16    uint16        `validate:"min:10,max:20"`
		UInt32    uint32        `validate:"min:10,max:20"`
		UInt64    uint32        `validate:"min:10,max:20"`
		String1   string        `validate:"required"`
		String2   string        `validate:"minLen:10,maxLen:100"`
		Duration1 time.Duration `validate:"min:10s,max:100s"`
		Duration2 time.Duration `validate:"required"`
		Duration3 time.Duration `validate:"positive"`
		Float32   float32       `validate:"min:3.2,max:6.4"`
		Float64   float32       `validate:"required,positive"`
		Bytes     utils.Bytes   `validate:"min:1MB,max:1GB"`
		Nested1   Nested1       `validate:"required"`
		Nested2   Nested2       `validate:"required"`
		Nested3   Nested3
	}

	It("Should validate with success", func() {
		target := Validate{
			Int:       -1,
			Int8:      -8,
			Int16:     -16,
			Int32:     -32,
			Int64:     -64,
			UInt:      10,
			UInt8:     11,
			UInt16:    13,
			UInt32:    15,
			UInt64:    20,
			String1:   "required",
			String2:   "min_len_10:0123456789",
			Duration1: 50 * time.Second,
			Duration2: time.Minute,
			Duration3: time.Second,
			Float32:   3.2,
			Float64:   6.4,
			Bytes:     "1MB",
			Nested1: Nested1{
				Int:      -101,
				Duration: 100 * time.Millisecond,
			},
			Nested2: Nested2{
				Float64: 10,
				Uint:    1000,
				Nested1: Nested1{
					Int:      -1000,
					Duration: time.Second,
				},
			},
			Nested3: Nested3{
				Duration: time.Second - 1,
				Uint16:   44,
				Nested1: Nested1{
					Int:      -2000,
					Duration: time.Hour,
				},
				Nested22: &Nested2{
					Float64: 66,
					Nested1: Nested1{
						Int:      -111,
						Duration: time.Second,
					},
				},
			},
		}

		Expect(validation.Validate(&target)).To(BeNil())
		Expect(validation.Validate(target)).To(BeNil())
	})

	It("Should fail validation", func() {
		target := Validate{
			Int:       0,
			Int8:      127,
			Int16:     1000,
			Int32:     10000,
			Int64:     100000,
			UInt:      1,
			UInt8:     1,
			UInt16:    1,
			UInt32:    100,
			UInt64:    100,
			String1:   "",
			String2:   "123",
			Duration1: time.Second,
			Duration2: 0,
			Duration3: -time.Second,
			Float32:   2.2,
			Float64:   -1,
			Bytes:     "1KB",
			Nested1: Nested1{
				Int:      -99,
				Duration: 99 * time.Millisecond,
			},
			Nested2: Nested2{
				Float64: 9,
				Uint:    1001,
			},
			Nested3: Nested3{
				Duration: time.Second + 1,
				Uint16:   0,
			},
		}

		expected := `validation failed: Int: is zero, Int8: is greater than 100, Int16: is greater than 100, Int32: is greater than 100, Int64: is greater than 100, UInt: is less than 10, UInt8: is less than 10, UInt16: is less than 10, UInt32: is greater than 20, UInt64: is greater than 20, String1: is empty, String2: length is less than 10, Duration1: is less than 10s, Duration2: is zero, Duration3: is less than 0s, Float32: is less than 3.2, Float64: is less than 0, Bytes: is less than 1MB, Nested1.Int: is greater than -100, Nested1.Duration: is less than 100ms, Nested2.Float64: is less than 10, Nested2.Uint: is greater than 1000, Nested2.Nested1.Int: is greater than -100, Nested2.Nested1.Duration: is less than 100ms, Nested2.Nested1: not set, Nested3.Duration: is greater than 1s, Nested3.Uint16: is zero, Nested3.Nested1.Int: is greater than -100, Nested3.Nested1.Duration: is less than 100ms, Nested3.Nested1: not set, Nested3.Nested22: is nil`

		err := validation.Validate(&target)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(Equal(expected))
	})

	It("Should return error for invalid validation spec", func() {
		type Validate struct {
			Int       int           `validate:"invalid"`
			Duration  time.Duration `validate:"min:5s,abcd"`
			Duration2 time.Duration `validate:"min:5z"`
			Duration3 time.Duration `validate:"max:50z"`
			String    string        `validate:"unk"`
			Int2      int           `validate:"minLen:10"`
		}
		target := Validate{}
		expectetd := "validation failed: Int: unknown validator invalid, Duration: unknown validator abcd, Duration2: invalid min value, Duration3: invalid max value, String: unknown validator unk, Int2: does not have length"

		err := validation.Validate(target)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(Equal(expectetd))
	})

	It("Should return error for non structs", func() {
		v1 := 77
		v2 := map[string]int32{}
		v3 := make(chan int)
		var v4 any = v2

		Expect(validation.Validate(&v1)).NotTo(BeNil())
		Expect(validation.Validate(&v2)).NotTo(BeNil())
		Expect(validation.Validate(&v3)).NotTo(BeNil())
		Expect(validation.Validate(&v4)).NotTo(BeNil())
	})

	It("Should validate WithCanValidate", func() {
		v := WithCanValidate{
			Value: "0123456789",
			Nested: WithCanValidateNested{
				Value: strings.Repeat("abc", 50),
			},
		}

		Expect(validation.Validate(v)).To(BeNil())
	})

	It("Should fail when WithCanValidate is invalid", func() {
		v := WithCanValidate{
			Value: "invalid",
			Nested: WithCanValidateNested{
				Value: "invalid",
			},
			Nested2: WithCanValidateNested{
				Value: "invalid",
			},
		}

		err := validation.Validate(v)
		expected := "validation failed: root is invalid, Nested: nested is invalid, Nested.Value: length is less than 100"

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(Equal(expected))
	})
})
