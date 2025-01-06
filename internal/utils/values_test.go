package utils_test

import (
	"time"

	"github.com/bcrusu/scout/internal/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Values tests", func() {
	It("Should set the provided values", func() {
		type Nested1 struct {
			Field1 int
			Field2 string
			NotSet int
		}
		type Nested2 struct {
			Field3  float64
			Field4  string
			Nested1 Nested1
			NotSet  bool
		}
		type Nested3 struct {
			Field5  time.Duration
			Field6  bool
			Nested1 Nested1
			Nested2 Nested2
			NotSet  string
		}
		type Values struct {
			FieldA  int
			FieldB  uint32
			FieldC  []byte
			Nested1 Nested1
			Nested2 Nested2
			Nested3 Nested3
			NotSet  string
		}

		values := map[string]any{
			"FieldA":                         7,
			"FieldB":                         uint32(9),
			"FieldC":                         []byte("bytes_value"),
			"Nested1.Field1":                 11,
			"Nested1.Field2":                 "12",
			"Nested2.Field3":                 3.14,
			"Nested2.Field4":                 "24",
			"Nested2.Nested1.Field1":         211,
			"Nested2.Nested1.Field2":         "212",
			"Nested3.Field5":                 5 * time.Second,
			"Nested3.Field6":                 true,
			"Nested3.Nested1.Field1":         311,
			"Nested3.Nested1.Field2":         "312",
			"Nested3.Nested2.Field3":         3.1415,
			"Nested3.Nested2.Field4":         "324",
			"Nested3.Nested2.Nested1.Field1": 3211,
			"Nested3.Nested2.Nested1.Field2": "3212",
		}

		target := Values{}
		expected := Values{
			FieldA: 7,
			FieldB: 9,
			FieldC: []byte("bytes_value"),
			Nested1: Nested1{
				Field1: 11,
				Field2: "12",
			},
			Nested2: Nested2{
				Field3: 3.14,
				Field4: "24",
				Nested1: Nested1{
					Field1: 211,
					Field2: "212",
				},
			},
			Nested3: Nested3{
				Field5: 5 * time.Second,
				Field6: true,
				Nested1: Nested1{
					Field1: 311,
					Field2: "312",
				},
				Nested2: Nested2{
					Field3: 3.1415,
					Field4: "324",
					Nested1: Nested1{
						Field1: 3211,
						Field2: "3212",
					},
				},
			},
		}

		err := utils.SetValues(&target, values)
		Expect(err).To(BeNil())
		Expect(target).To(Equal(expected))
	})

	It("Should return error for unknown fields", func() {
		type Values struct {
			Field1 int
			Field2 string
		}
		target := Values{}

		values := map[string]any{
			"Field1":  77,
			"Field2":  "22",
			"Unknown": 123,
		}

		err := utils.SetValues(&target, values)
		Expect(err).NotTo(BeNil())
	})

	It("Should return error for non-matching value type", func() {
		type Values struct {
			Field1 int
			Field2 int
		}
		target := Values{}

		values := map[string]any{
			"Field1": 77,
			"Field2": "7777",
		}

		err := utils.SetValues(&target, values)
		Expect(err).NotTo(BeNil())
	})
})
