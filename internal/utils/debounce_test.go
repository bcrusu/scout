package utils_test

import (
	"context"
	"time"

	"github.com/bcrusu/graph/internal/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = DescribeTableSubtree("Debounce tests", func(chanSize int) {
	pause := 10 * time.Millisecond

	It("Should send only after configured pause", func() {
		ch := make(chan int, chanSize)
		db := utils.DebounceChan(context.Background(), ch, pause)

		go func() {
			// send 10 bursts of 5 messages with pause between bursts
			for i := range 10 {
				for range 5 {
					ch <- i
					time.Sleep(pause / 3)
				}
				time.Sleep(pause * 2)
			}
			close(ch)
		}()

		for i := range 10 {
			Expect(<-db).To(Equal(i))
		}

		Eventually(db).Should(BeClosed())
	})

	It("Should also send last message after chan is closed", func() {
		ch := make(chan int, chanSize)
		db := utils.DebounceChan(context.Background(), ch, pause)

		go func() {
			ch <- 1
			ch <- 2
			time.Sleep(pause * 2)
			ch <- 3
			ch <- 4
			ch <- 5
			time.Sleep(pause * 2)
			ch <- 6
			ch <- 7
			close(ch)
		}()

		Expect(<-db).To(Equal(2))
		Expect(<-db).To(Equal(5))
		Expect(<-db).To(Equal(7))
		Eventually(db).Should(BeClosed())
	})

	It("Should not leak goroutine when ctx is canceled", func() {
		cctx, cancel := context.WithCancel(context.Background())
		ch := make(chan int, chanSize)
		db := utils.DebounceChan(cctx, ch, pause)

		go func() {
			ch <- 1
		}()

		time.Sleep(10 * time.Millisecond)
		cancel()
		Eventually(db).Should(BeClosed())
	})

	It("Should work with nil", func() {
		ch := make(chan any, chanSize)
		db := utils.DebounceChan(context.Background(), ch, pause)

		go func() {
			ch <- 1
			ch <- nil
			time.Sleep(pause * 2)
			ch <- nil
			ch <- 2
			close(ch)
		}()

		Expect(<-db).To(BeNil())
		Expect(<-db).To(Equal(2))
		Eventually(db).Should(BeClosed())
	})
},
	Entry("chan with no buffer", 0),
	Entry("chan with buffer", 5),
)
