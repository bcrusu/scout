package utils_test

import (
	"time"

	"github.com/bcrusu/scout/internal/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("PubSub tests", func() {
	shortPause := func() <-chan time.Time {
		return time.After(10 * time.Millisecond)
	}

	Context("When there are no subscribers", func() {
		It("Publisher should not block", func() {
			pub := utils.NewPubSub[int](10)

			pub.Publish(1)
			pub.PublishAttempt(1)
		})
	})

	Describe("Buffered subscribers", func() {
		const bufferSize = 10
		var (
			pub  utils.Publisher[int]
			sub1 utils.Subscriber[int]
			sub2 utils.Subscriber[int]
		)

		BeforeEach(func() {
			pub = utils.NewPubSub[int](bufferSize)
			sub1 = pub.Subscribe(bufferSize)
			sub2 = pub.Subscribe(bufferSize)

			Expect(pub).NotTo(BeNil())
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
					sub1.Item()
					close(doneCh)
				}()

				select {
				case <-doneCh:
					Fail("Subscriber did not wait")
				case <-shortPause():
					pub.Publish(1)
				}
			})

			It("Publish should succeed", func() {
				pub.Publish(88)
				Expect(sub1.Item()).To(Equal(88))
				Expect(sub2.Item()).To(Equal(88))
			})

			It("Publish attempt should succeed", func() {
				Expect(pub.PublishAttempt(99)).To(BeTrue())
				Expect(sub1.Item()).To(Equal(99))
				Expect(sub2.Item()).To(Equal(99))
			})
		})

		Context("When buffer has items", func() {
			It("Should consume in order", func() {
				pub.Publish(3)
				pub.Publish(2)
				pub.Publish(1)

				Expect(sub1.Item()).To(Equal(3))
				Expect(sub1.Item()).To(Equal(2))
				Expect(sub1.Item()).To(Equal(1))
			})

			It("Should consume in order using chan", func() {
				pub.Publish(3)
				pub.Publish(2)
				pub.Publish(1)

				Expect(<-sub1.ItemChan()).To(Equal(3))
				Expect(<-sub1.ItemChan()).To(Equal(2))
				Expect(<-sub1.ItemChan()).To(Equal(1))
			})
		})

		Context("When buffer is full", func() {
			BeforeEach(func() {
				for i := range bufferSize {
					pub.Publish(i)
				}
			})

			It("Publish should block waiting for subscribers", func() {
				doneCh := make(chan bool)

				go func() {
					pub.Publish(99)
					close(doneCh)
				}()

				select {
				case <-doneCh:
					Fail("Publisher did not wait")
				case <-shortPause():
					sub1.Item()
					sub2.Item()
				}
			})

			It("Publish attempt should fail", func() {
				Expect(pub.PublishAttempt(99)).To(BeFalse())
			})
		})
	})

	Describe("Unbuffered subscribers", func() {
		var (
			pub utils.Publisher[int]
			sub utils.Subscriber[int]
		)

		BeforeEach(func() {
			pub = utils.NewPubSub[int](0)
			sub = pub.Subscribe(0)

			Expect(pub).NotTo(BeNil())
			Expect(sub).NotTo(BeNil())
		})

		It("Subscriber should wait for publisher", func() {
			doneCh := make(chan bool)

			go func() {
				pub.Publish(1)
				close(doneCh)
			}()

			select {
			case <-doneCh:
				Fail("Subscriber did not wait.")
			case <-sub.ItemChan():
			}
		})

		It("Publisher should wait for subscriber", func() {
			doneCh := make(chan bool)
			go func() {
				pub.Publish(99)
				close(doneCh)
			}()

			select {
			case <-doneCh:
				Fail("Publisher did not wait")
			case <-shortPause():
				sub.Item()
			}
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
			pub := utils.NewPubSub[int](10)
			sub := pub.Subscribe(0)

			sub.Unsubscribe()
			r1 := catchPanic(func() { sub.Item() })
			r2 := catchPanic(func() { <-sub.ItemChan() })
			r3 := catchPanic(func() { sub.Unsubscribe() })
			r4 := catchPanic(func() { sub.NotifyPublisher() })
			Expect(r1).To(BeTrue())
			Expect(r2).To(BeTrue())
			Expect(r3).To(BeTrue())
			Expect(r4).To(BeTrue())
		})
	})

	Context("When all subscribers unsubscribe", func() {
		It("Publish should not block", func() {
			pub := utils.NewPubSub[int](10)
			sub1 := pub.Subscribe(0)
			sub2 := pub.Subscribe(0)

			sub1.Unsubscribe()
			sub2.Unsubscribe()
			pub.Publish(77)
		})
	})

	Context("When subscriber notifies the publisher", func() {
		It("Publisher should receive", func() {
			pub := utils.NewPubSub[int](10)
			sub := pub.Subscribe(0)
			sub.NotifyPublisher()

			select {
			case <-pub.NotifyChan():
			case <-shortPause():
				Fail("Publisher was not notified")
			}
		})

		It("Publisher should wait to be notified", func() {
			pub := utils.NewPubSub[int](10)

			select {
			case <-pub.NotifyChan():
				Fail("Publisher did not wait")
			case <-shortPause():
			}
		})
	})
})
