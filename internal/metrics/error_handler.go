package metrics

import (
	"go.opentelemetry.io/otel"
)

var (
	_ otel.ErrorHandler = (*errorHandler)(nil)
)

type errorHandler struct{}

func (h *errorHandler) Handle(err error) {
	log.WithError(err).Error("Internal otel error.")
}
