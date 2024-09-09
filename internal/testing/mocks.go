package testing

import (
	"fmt"

	"github.com/onsi/ginkgo"
	"go.uber.org/mock/gomock"
)

func NewMockController() *gomock.Controller {
	var ctrl *gomock.Controller

	ginkgo.BeforeEach(func() {
		reporter := testReporter{}
		ctrl = gomock.NewController(reporter)
	})

	ginkgo.AfterEach(func() {
		ctrl.Finish()
	})

	return ctrl
}

type testReporter struct{}

func (r testReporter) Errorf(format string, args ...interface{}) {
	ginkgo.Fail(fmt.Sprintf(format, args...))
}

func (r testReporter) Fatalf(format string, args ...interface{}) {
	ginkgo.Fail(fmt.Sprintf(format, args...))
}
