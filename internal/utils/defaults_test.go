package utils_test

import (
	"time"

	"github.com/bcrusu/scout/internal/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Defaults tests", func() {
	It("Should set defaults for empty fields", func() {
		type Nested1 struct {
			Int    int    `default:"1000"`
			String string `default:"nested1"`
		}
		type Nested2 struct {
			Bool    bool    `default:"true"`
			Float64 float64 `default:"6.9"`
			Nested1 Nested1
		}
		type Nested3 struct {
			Duration time.Duration `default:"99s"`
			Uint16   uint16        `default:"44"`
			Nested1  Nested1
			Nested2  Nested2
		}
		type Defaults struct {
			Int             int           `default:"-10"`
			Int8            int8          `default:"-8"`
			Int16           int16         `default:"-16"`
			Int32           int32         `default:"-32"`
			Int64           int32         `default:"-64"`
			UInt            uint          `default:"10"`
			UInt8           uint8         `default:"8"`
			UInt16          uint16        `default:"16"`
			UInt32          uint32        `default:"32"`
			UInt64          uint32        `default:"64"`
			BoolTrue        bool          `default:"true"`
			BoolFalse       bool          `default:"false"`
			String          string        `default:"default_string_value"`
			Duration_5s     time.Duration `default:"5s"`
			Duration_10m20s time.Duration `default:"10m20s"`
			Float32         float32       `default:"3.2"`
			Float64         float32       `default:"6.4"`
			Bytes           utils.Bytes   `default:"5MB"`
			Nested1         Nested1
			Nested2         Nested2
			Nested3         Nested3
		}

		target := Defaults{}
		expected := Defaults{
			Int:             -10,
			Int8:            -8,
			Int16:           -16,
			Int32:           -32,
			Int64:           -64,
			UInt:            10,
			UInt8:           8,
			UInt16:          16,
			UInt32:          32,
			UInt64:          64,
			BoolTrue:        true,
			BoolFalse:       false,
			String:          "default_string_value",
			Duration_5s:     5 * time.Second,
			Duration_10m20s: 10*time.Minute + 20*time.Second,
			Float32:         3.2,
			Float64:         6.4,
			Bytes:           "5MB",
			Nested1: Nested1{
				Int:    1000,
				String: "nested1",
			},
			Nested2: Nested2{
				Bool:    true,
				Float64: 6.9,
				Nested1: Nested1{
					Int:    1000,
					String: "nested1",
				},
			},
			Nested3: Nested3{
				Duration: 99 * time.Second,
				Uint16:   44,
				Nested1: Nested1{
					Int:    1000,
					String: "nested1",
				},
				Nested2: Nested2{
					Bool:    true,
					Float64: 6.9,
					Nested1: Nested1{
						Int:    1000,
						String: "nested1",
					},
				},
			},
		}

		err := utils.SetDefaults(&target)
		Expect(err).To(BeNil())
		Expect(target).To(Equal(expected))
	})

	It("Should skip fields with non-zero values", func() {
		type Nested struct {
			Int    int    `default:"1000"`
			String string `default:"nested"`
		}
		type Defaults struct {
			Int    int    `default:"-10"`
			String string `default:"abc"`
			Bool   bool   `default:"true"`
			Nested Nested
		}

		target := Defaults{
			Int:  44,
			Bool: false, // will be overwritten to default true value even though the intention was to set it to false
			Nested: Nested{
				Int: 0, // similar, will be overwritten
			},
		}
		expected := Defaults{
			Int:    44,
			String: "abc",
			Bool:   true,
			Nested: Nested{
				Int:    1000,
				String: "nested",
			},
		}

		err := utils.SetDefaults(&target)
		Expect(err).To(BeNil())
		Expect(target).To(Equal(expected))
	})

	It("Should return error for invalid default value", func() {
		type Defaults struct {
			Int      int           `default:"invalid_int"`
			Duration time.Duration `default:"invalid_duration"`
		}
		target := Defaults{}
		expected := Defaults{}

		err := utils.SetDefaults(&target)
		Expect(err).NotTo(BeNil())
		Expect(target).To(Equal(expected))
	})

	It("Should return error for non structs", func() {
		v1 := 77
		v2 := map[string]int32{}
		v3 := make(chan int)
		var v4 any = v2

		Expect(utils.SetDefaults(&v1)).NotTo(BeNil())
		Expect(utils.SetDefaults(&v2)).NotTo(BeNil())
		Expect(utils.SetDefaults(&v3)).NotTo(BeNil())
		Expect(utils.SetDefaults(&v4)).NotTo(BeNil())
	})
})
