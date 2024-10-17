package eventbus_test

import (
	"testing"
	"time"

	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/utils/tests"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSuite(t *testing.T) {
	tests.NewSuite(t, "eventbus test suite")
}

var _ = Describe("MessageBus tests", func() {
	type testEvent struct {
		payload string
	}

	bufferSize := 10

	shortPause := func() <-chan time.Time {
		return time.After(10 * time.Millisecond)
	}

	Context("When there are no subscribers", func() {
		It("Publisher should not block", func() {
			eventbus.SetMessageBus(eventbus.NewMessageBus())

			eventbus.Publish(1)
			eventbus.TryPublish(1)
		})

		It("Should store last message published and send it to new subscribers", func() {
			eventbus.SetMessageBus(eventbus.NewMessageBus())

			eventbus.Publish[int32](32)
			eventbus.Publish[uint8](17)
			eventbus.Publish("test1")
			eventbus.Publish(testEvent{"hello!"})
			eventbus.Publish[*testEvent](nil)
			sub1 := eventbus.Subscribe[int32]()
			sub2 := eventbus.Subscribe[uint8]()
			sub3 := eventbus.Subscribe[string]()
			sub4 := eventbus.Subscribe[testEvent]()
			sub5 := eventbus.Subscribe[*testEvent]()

			Expect(<-sub1.Items()).To(Equal(int32(32)))
			Expect(<-sub2.Items()).To(Equal(uint8(17)))
			Expect(<-sub3.Items()).To(Equal("test1"))
			Expect(<-sub4.Items()).To(Equal(testEvent{"hello!"}))
			Expect(<-sub5.Items()).To(BeNil())
		})
	})

	Context("When there are subscribers", func() {
		var (
			sub1 *eventbus.Subscriber[int]
			sub2 *eventbus.Subscriber[int]
		)

		BeforeEach(func() {
			eventbus.SetMessageBus(eventbus.NewMessageBus())
			sub1 = eventbus.Subscribe[int](bufferSize)
			sub2 = eventbus.Subscribe[int](bufferSize)

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
					eventbus.TryPublish(1)
				}
			})

			It("Publish should succeed", func() {
				go eventbus.Publish(88)
				Expect(<-sub1.Items()).To(Equal(88))
				Expect(<-sub2.Items()).To(Equal(88))
			})

			It("TryPublish should succeed", func() {
				go eventbus.TryPublish(88)
				Expect(<-sub1.Items()).To(Equal(88))
				Expect(<-sub2.Items()).To(Equal(88))
			})
		})

		Context("When buffer has items", func() {
			It("Should consume in order", func() {
				eventbus.Publish(3)
				eventbus.Publish(2)
				eventbus.Publish(1)

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
					eventbus.Publish(i)
				}
			})

			It("Publish should block waiting for subscribers", func() {
				doneCh := make(chan bool)

				go func() {
					eventbus.Publish(99)
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
				Expect(eventbus.TryPublish(99)).To(BeFalse())
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
			eventbus.SetMessageBus(eventbus.NewMessageBus())
			sub := eventbus.Subscribe[int](10)
			sub.Unsubscribe()
			r1 := catchPanic(func() { <-sub.Items() })
			r2 := catchPanic(func() { sub.Unsubscribe() })
			Expect(r1).To(BeTrue())
			Expect(r2).To(BeTrue())
		})
	})

	Context("When all subscribers unsubscribe", func() {
		It("Publish should not block", func() {
			eventbus.SetMessageBus(eventbus.NewMessageBus())
			sub1 := eventbus.Subscribe[int](10)
			sub2 := eventbus.Subscribe[int](10)
			sub1.Unsubscribe()
			sub2.Unsubscribe()

			eventbus.Publish(77)
		})
	})
})
