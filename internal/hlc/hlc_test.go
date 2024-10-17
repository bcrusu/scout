package hlc_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/bcrusu/scout/internal/hlc"
	"github.com/bcrusu/scout/internal/utils/tests"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSuite(t *testing.T) {
	tests.NewSuite(t, "HLC test suite")
}

var _ = Describe("HLC tests", func() {
	Context("Now", func() {
		It("Should return current timestamp", func() {
			hlc := hlc.New(time.Second)
			ts := hlc.Now()
			Expect(ts > 0).To(BeTrue())
		})
	})

	Context("Update too far in the future", func() {
		It("Should return error", func() {
			hlc := hlc.New(time.Second)
			ts := hlc.Now() + uint64(2*time.Second)
			err := hlc.Update(ts)
			Expect(err).NotTo(BeNil())
		})
	})
})

// BenchmarkHlcNowParallel-8   	10165191	       113.5 ns/op	       0 B/op	       0 allocs/op
// {Total:10165191 LogicalReset:17609 LogicalInc:10147582 BackwardJumps:0 HitLogicalMax:0}
func BenchmarkHlcNowParallel(b *testing.B) {
	hlc := hlc.New(time.Second)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hlc.Now()
		}
	})
	b.StopTimer()

	s, _ := hlc.Stats()
	fmt.Printf("%+v\n", s)
}

// BenchmarkHlcNowSerial-8   	22382936	        51.33 ns/op	       0 B/op	       0 allocs/op
// {Total:22382936 LogicalReset:17531 LogicalInc:22365405 BackwardJumps:0 HitLogicalMax:0}
func BenchmarkHlcNowSerial(b *testing.B) {
	hlc := hlc.New(time.Second)
	b.ResetTimer()

	for range b.N {
		hlc.Now()
	}
	b.StopTimer()

	s, _ := hlc.Stats()
	fmt.Printf("%+v\n", s)
}

// BenchmarkHlcUpdateParallel-8   	5023106	       241.0 ns/op	       0 B/op	       0 allocs/op
// {Total:5023106 OutOfRange:0 LogicalReset:9023 LogicalTies:4966811 LogicalOurs:47272 LogicalTheirs:0 HitLogicalMax:0}
func BenchmarkHlcUpdateParallel(b *testing.B) {
	hlc := hlc.New(time.Second)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hlc.Update(hlc.Now())
		}
	})
	b.StopTimer()

	_, s := hlc.Stats()
	fmt.Printf("%+v\n", s)
}

// BenchmarkHlcUpdateSerial-8   	9332781	       120.0 ns/op	       0 B/op	       0 allocs/op
// {Total:9332781 OutOfRange:0 LogicalReset:8535 LogicalTies:9324246 LogicalOurs:0 LogicalTheirs:0 HitLogicalMax:0}
func BenchmarkHlcUpdateSerial(b *testing.B) {
	hlc := hlc.New(time.Second)
	b.ResetTimer()

	for range b.N {
		hlc.Update(hlc.Now())
	}
	b.StopTimer()

	_, s := hlc.Stats()
	fmt.Printf("%+v\n", s)
}
