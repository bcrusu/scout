package utils_test

import (
	"time"

	"github.com/bcrusu/graph/internal/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Throttle tests", func() {
	It("Should send only once per interval", func() {
		ch := make(chan int)
		th := utils.ThrottleChan(ch, 10*time.Second)

		go func() {
			for i := range 100 {
				ch <- 1000 + i
			}
			close(ch)
		}()

		Expect(<-th).To(Equal(1000))
		Eventually(th).Should(BeEmpty())
		Eventually(th).Should(BeClosed())
	})

	It("Should send after interval", func() {
		interval := 2 * time.Millisecond
		ch := make(chan int)
		th := utils.ThrottleChan(ch, interval)

		counter := 0
		go func() {
			for range th {
				counter++
			}
		}()

		expected := 5
		for i := range expected {
			ch <- i
			time.Sleep(2 * interval)
		}

		Expect(counter).To(Equal(expected))
		close(ch)
	})
})
