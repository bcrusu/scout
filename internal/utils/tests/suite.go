package tests

import (
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
)

func NewSuite(t *testing.T, description string) {
	gomega.RegisterFailHandler(ginkgo.Fail)

	ginkgo.AfterEach(func() {
		gomega.Eventually(gleak.Goroutines).ShouldNot(gleak.HaveLeaked())
	})

	ginkgo.RunSpecs(t, description)
}
