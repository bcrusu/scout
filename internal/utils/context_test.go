package utils_test

import (
	"context"

	"github.com/bcrusu/scout/internal/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("WithCancelAndWait tests", func() {
	It("Should wait for work to complete", func() {
		called := false
		work, cancel := utils.WithCancelAndWait(func(context.Context) {
			called = true
		})

		go work(context.Background())

		cancel()
		Expect(called).To(BeTrue())
	})

	It("Should wait for work to complete and return result", func() {
		work, cancel := utils.WithCancelAndWaitR(func(context.Context) int {
			return 99
		})

		result := 0
		go func() {
			result = work(context.Background())
		}()

		cancel()
		Expect(result).To(Equal(99))
	})
})

var _ = Describe("WithCancel tests", func() {
	It("Should cancel the work and not wait", func() {
		result := 0
		work, cancel := utils.WithCancel(func(ctx context.Context) {
			<-ctx.Done()
			result = 88
		})

		go cancel()

		work(context.Background())
		Expect(result).To(Equal(88))
	})
})
