package utils_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gmeasure"
)

func newTestDrainer() *utils.Drainer {
	return utils.NewDrainer(logging.New("drainer_tests"))
}

var _ = Describe("Drainer tests", func() {
	Context("When stopping", func() {
		It("Should drain all in-flight work", func() {
			drainer := newTestDrainer()

			var wg sync.WaitGroup
			wg.Add(10)

			drainedCount := int32(0)

			for range 10 {
				go func() {
					ctx, cancel := drainer.WithDrain(context.Background())
					defer cancel()
					wg.Done()
					<-ctx.Done()
					atomic.AddInt32(&drainedCount, 1)
				}()
			}

			wg.Wait()
			drainer.Stop()
			Expect(drainedCount).To(BeEquivalentTo(10))
		})
	})

	Context("When no work is in-flight", func() {
		It("Stop should return", func() {
			drainer := newTestDrainer()

			_, cancel := drainer.WithDrain(context.Background())
			cancel()

			drainer.Stop()
		})
	})

	Context("When draining", func() {
		It("WithDrain should return canceled context", func() {
			drainer := newTestDrainer()
			longRunning := make(chan any)

			go func() {
				_, cancel := drainer.WithDrain(context.Background())
				defer cancel()
				<-longRunning
			}()

			go drainer.Stop()

			time.Sleep(10 * time.Millisecond)

			ctx, cancel := drainer.WithDrain(context.Background())
			defer cancel()

			Expect(ctx.Done()).To(BeClosed())
			Expect(cancel).NotTo(BeNil())
			close(longRunning)
		})
	})

	Context("When stoppped", func() {
		It("WithDrain return canceled context", func() {
			drainer := newTestDrainer()
			drainer.Stop()

			ctx, cancel := drainer.WithDrain(context.Background())
			Expect(ctx.Done()).To(BeClosed())
			Expect(cancel).NotTo(BeNil())
			cancel()
		})
	})

	Context("Benchmark", func() {
		It("Should be faster", func() {
			ex := NewExperiment("bench")
			AddReportEntry(ex.Name, ex)

			drainer := newTestDrainer()

			ex.SampleDuration("time",
				func(idx int) {
					_, cancel := drainer.WithDrain(context.Background())
					cancel()
				}, SamplingConfig{
					N:           1000000,
					Duration:    time.Second / 2,
					NumParallel: 5000,
				})

			drainer.Stop()
		})
	})
})

func BenchmarkDrainer(b *testing.B) {
	drainer := newTestDrainer()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, cancel := drainer.WithDrain(context.Background())
			cancel()
		}
	})

	drainer.Stop()
}
