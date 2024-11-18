package validation

import (
	"fmt"
	"math"
	"reflect"
)

func port(val reflect.Value, param string) string {
	if param != "" {
		return "unexpectetd param value"
	}

	var port int

	switch {
	case val.CanInt():
		port = int(val.Int())
	case val.CanUint():
		port = int(val.Uint())
	default:
		return "unsupported type"
	}

	if port <= 0 || port > math.MaxUint16 {
		return fmt.Sprintf("invalid port %d", port)
	}

	return ""
}
