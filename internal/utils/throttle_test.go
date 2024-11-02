package utils_test

import (
	"context"
	"time"

	"github.com/bcrusu/scout/internal/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = DescribeTableSubtree("Throttle tests", func(chanSize int) {
	It("Should send only once per interval", func() {
		ch := make(chan int, chanSize)
		th := utils.ThrottleChan(context.Background(), ch, 10*time.Second)

		go func() {
			for i := range 100 {
				ch <- 1000 + i
			}
			close(ch)
		}()

		Expect(<-th).To(Equal(1000))
		Eventually(th).Should(BeEmpty())
	})

	It("Should send after interval", func() {
		interval := 2 * time.Millisecond
		ch := make(chan int, chanSize)
		th := utils.ThrottleChan(context.Background(), ch, interval)
		done := make(chan any)

		counter := 0
		go func() {
			for {
				select {
				case <-th:
					counter++
				case <-done:
					return
				}
			}
		}()

		expected := 5
		for i := range expected {
			ch <- i
			time.Sleep(2 * interval)
		}

		Expect(counter).To(Equal(expected))
		close(ch)
		close(done)
	})

	It("Should not leak goroutine when chan is closed", func() {
		ch := make(chan int, chanSize)
		th := utils.ThrottleChan(context.Background(), ch, 10*time.Millisecond)

		go func() {
			ch <- 1
			close(ch)
		}()

		Expect(<-th).To(Equal(1))
	})

	It("Should not leak goroutine when ctx is canceled", func() {
		cctx, cancel := context.WithCancel(context.Background())
		ch := make(chan int, chanSize)
		th := utils.ThrottleChan(cctx, ch, 10*time.Millisecond)

		go func() {
			ch <- 1
		}()

		go func() {
			<-th
		}()

		time.Sleep(10 * time.Millisecond)
		cancel()
	})

	It("Should work with MakeThrottleChan", func() {
		ch, th := utils.MakeThrottleChan[int](10*time.Second, chanSize)

		go func() {
			for i := range 100 {
				ch <- 1000 + i
			}
			close(ch)
		}()

		Expect(<-th).To(Equal(1000))
		Eventually(th).Should(BeEmpty())
	})
},
	Entry("chan with no buffer", 0),
	Entry("chan with buffer", 5),
)
