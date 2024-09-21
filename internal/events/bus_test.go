package events_test

import (
	"testing"
	"time"

	"github.com/bcrusu/graph/internal/events"
	"github.com/bcrusu/graph/internal/utils/tests"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUtils(t *testing.T) {
	tests.NewSuite(t, "validation test suite")
}

var _ = Describe("MessageBus tests", func() {
	bufferSize := 10

	shortPause := func() <-chan time.Time {
		return time.After(10 * time.Millisecond)
	}

	Context("When there are no subscribers", func() {
		It("Publisher should not block", func() {
			events.SetMessageBus(events.NewMessageBus())

			events.Publish(1)
			events.TryPublish(1)
		})
	})

	Context("When there are subscribers", func() {
		var (
			sub1 *events.Subscriber[int]
			sub2 *events.Subscriber[int]
		)

		BeforeEach(func() {
			events.SetMessageBus(events.NewMessageBus())
			sub1 = events.Subscribe[int](bufferSize)
			sub2 = events.Subscribe[int](bufferSize)

			Expect(sub1).NotTo(BeNil())
			Expect(sub2).NotTo(BeNil())
		})

		AfterEach(func() {
			sub1.Unsubscribe()
			sub2.Unsubscribe()
		})

		Context("When the buffer is empy", func() {
			It("Subscriber should wait for publisher", func() {
				doneCh := make(chan bool)

				go func() {
					<-sub1.Items()
					close(doneCh)
				}()

				select {
				case <-doneCh:
					Fail("Subscriber did not wait")
				case <-shortPause():
					events.TryPublish(1)
				}
			})

			It("Publish should succeed", func() {
				go events.Publish(88)
				Expect(<-sub1.Items()).To(Equal(88))
				Expect(<-sub2.Items()).To(Equal(88))
			})

			It("TryPublish should succeed", func() {
				go events.TryPublish(88)
				Expect(<-sub1.Items()).To(Equal(88))
				Expect(<-sub2.Items()).To(Equal(88))
			})
		})

		Context("When buffer has items", func() {
			It("Should consume in order", func() {
				events.Publish(3)
				events.Publish(2)
				events.Publish(1)

				Expect(<-sub1.Items()).To(Equal(3))
				Expect(<-sub1.Items()).To(Equal(2))
				Expect(<-sub1.Items()).To(Equal(1))
				Expect(<-sub2.Items()).To(Equal(3))
				Expect(<-sub2.Items()).To(Equal(2))
				Expect(<-sub2.Items()).To(Equal(1))
			})
		})

		Context("When buffer is full", func() {
			BeforeEach(func() {
				for i := range bufferSize {
					events.Publish(i)
				}
			})

			It("Publish should block waiting for subscribers", func() {
				doneCh := make(chan bool)

				go func() {
					events.Publish(99)
					close(doneCh)
				}()

				select {
				case <-doneCh:
					Fail("Publisher did not wait")
				case <-shortPause():
					<-sub1.Items()
					<-sub2.Items()
				}
			})

			It("TryPublish should fail", func() {
				Expect(events.TryPublish(99)).To(BeFalse())
			})
		})
	})

	Context("When subscriber unsubscribes", func() {
		catchPanic := func(work func()) (flag bool) {
			defer func() {
				if r := recover(); r != nil {
					flag = true
				}
			}()

			work()
			return flag
		}

		It("Should panic trying any action", func() {
			events.SetMessageBus(events.NewMessageBus())
			sub := events.Subscribe[int](10)
			sub.Unsubscribe()
			r1 := catchPanic(func() { <-sub.Items() })
			r2 := catchPanic(func() { sub.Unsubscribe() })
			Expect(r1).To(BeTrue())
			Expect(r2).To(BeTrue())
		})
	})

	Context("When all subscribers unsubscribe", func() {
		It("Publish should not block", func() {
			events.SetMessageBus(events.NewMessageBus())
			sub1 := events.Subscribe[int](10)
			sub2 := events.Subscribe[int](10)
			sub1.Unsubscribe()
			sub2.Unsubscribe()

			events.Publish(77)
		})
	})
})
