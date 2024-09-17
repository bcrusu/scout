package common

import (
	"github.com/bcrusu/graph/internal/logging"
)

var (
	log = logging.WithComponent("data_common")
)

// Shared implements common functionality for both leader and follower roles.
type Shared struct {
}

func New() *Shared {
	return &Shared{}
}
