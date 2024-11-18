package validation

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/bcrusu/scout/internal/utils"
)

var (
	allValidators = map[string]valFn{
		"min":        min,
		"max":        max,
		"minLen":     minLen,
		"maxLen":     maxLen,
		"positive":   positive,
		"minItemLen": minItemLen,
		"maxItemLen": maxItemLen,
		"port":       port,
	}
)

type valFn func(v reflect.Value, params string) string

type validator struct {
	valFn valFn
	param string
}

func required(v reflect.Value) string {
	v = unwrap(v)

	switch {
	case canLen(v) && v.Len() == 0:
		return "is empty"
	case isNumeric(v) && isZero(v):
		return "is zero"
	case canIsNil(v) && v.IsNil():
		return "is nil"
	case v.Kind() == reflect.Struct && isZero(v):
		return "not set"
	}

	return ""
}

func min(val reflect.Value, param string) string {
	parseErr := "invalid min value"
	valErr := "is less than %v"

	switch x := unwrap(val).Interface().(type) {
	case time.Duration:
		if target, err := time.ParseDuration(param); err != nil {
			return parseErr
		} else if x < target {
			return fmt.Sprintf(valErr, target)
		}
	case utils.Bytes:
		bytes, err := x.Parse()
		if err != nil {
			return "invalid value"
		} else if target, err := utils.Bytes(param).Parse(); err != nil {
			return parseErr
		} else if bytes < target {
			return fmt.Sprintf(valErr, param)
		}
	default:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if target, err := strconv.ParseInt(param, 10, 64); err != nil {
				return parseErr
			} else if val.Int() < target {
				return fmt.Sprintf(valErr, target)
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if target, err := strconv.ParseUint(param, 10, 64); err != nil {
				return parseErr
			} else if val.Uint() < target {
				return fmt.Sprintf(valErr, target)
			}
		case reflect.Float32, reflect.Float64:
			if target, err := strconv.ParseFloat(param, 64); err != nil {
				return parseErr
			} else if val.Float() < target {
				return fmt.Sprintf(valErr, target)
			}
		default:
			return "unsupported type"
		}
	}
	return ""
}

func max(val reflect.Value, param string) string {
	parseErr := "invalid max value"
	valErr := "is greater than %v"

	switch x := unwrap(val).Interface().(type) {
	case time.Duration:
		if target, err := time.ParseDuration(param); err != nil {
			return parseErr
		} else if x > target {
			return fmt.Sprintf(valErr, target)
		}
	case utils.Bytes:
		bytes, err := x.Parse()
		if err != nil {
			return "invalid value"
		} else if target, err := utils.Bytes(param).Parse(); err != nil {
			return parseErr
		} else if bytes > target {
			return fmt.Sprintf(valErr, param)
		}
	default:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if target, err := strconv.ParseInt(param, 10, 64); err != nil {
				return parseErr
			} else if val.Int() > target {
				return fmt.Sprintf(valErr, target)
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if target, err := strconv.ParseUint(param, 10, 64); err != nil {
				return parseErr
			} else if val.Uint() > target {
				return fmt.Sprintf(valErr, target)
			}
		case reflect.Float32, reflect.Float64:
			if target, err := strconv.ParseFloat(param, 64); err != nil {
				return parseErr
			} else if val.Float() > target {
				return fmt.Sprintf(valErr, target)
			}
		default:
			return "unsupported type"
		}
	}
	return ""
}

func minLen(val reflect.Value, param string) string {
	target, err := strconv.ParseInt(param, 10, 0)
	if err != nil {
		return "invalid min length value"
	}

	if !canLen(val) {
		return "does not have length"
	}

	if val.Len() < int(target) {
		return fmt.Sprintf("length is less than %d", target)
	}

	return ""
}

func maxLen(val reflect.Value, param string) string {
	target, err := strconv.ParseInt(param, 10, 0)
	if err != nil {
		return "invalid max length value"
	}

	if !canLen(val) {
		return "does not have length"
	}

	if val.Len() > int(target) {
		return fmt.Sprintf("length is greater than %d", target)
	}

	return ""
}

func minItemLen(val reflect.Value, param string) string {
	target, err := strconv.ParseInt(param, 10, 0)
	if err != nil {
		return "invalid min item length value"
	}

	if !canIndex(val) {
		return "does not have items"
	}

	for i := 0; i < val.Len(); i++ {
		item := val.Index(i)

		if !canLen(item) {
			return fmt.Sprintf("item %d does not have length", i)
		} else if item.Len() < int(target) {
			return fmt.Sprintf("item %d length is less than %d", i, target)
		}
	}

	return ""
}

func maxItemLen(val reflect.Value, param string) string {
	target, err := strconv.ParseInt(param, 10, 0)
	if err != nil {
		return "invalid max item length value"
	}

	if !canIndex(val) {
		return "does not have items"
	}

	for i := 0; i < val.Len(); i++ {
		item := val.Index(i)

		if !canLen(item) {
			return fmt.Sprintf("item %d does not have length", i)
		} else if item.Len() > int(target) {
			return fmt.Sprintf("item %d length is greater than %d", i, target)
		}
	}

	return ""
}

func positive(val reflect.Value, param string) string {
	if param != "" {
		return "unexpectetd param value"
	}
	return min(val, "0")
}

func canIsNil(v reflect.Value) bool {
	k := v.Kind()
	return k == reflect.Chan || k == reflect.Func || k == reflect.Map || k == reflect.Ptr ||
		k == reflect.UnsafePointer || k == reflect.Interface || k == reflect.Slice
}

func canLen(v reflect.Value) bool {
	k := v.Kind()
	return k == reflect.Array || k == reflect.Map || k == reflect.Slice || k == reflect.String
}

func canIndex(v reflect.Value) bool {
	k := v.Kind()
	return k == reflect.Array || k == reflect.Slice || k == reflect.String
}

func isNumeric(v reflect.Value) bool {
	return v.CanUint() || v.CanInt() || v.CanFloat() || v.CanComplex()
}

func isZero(v reflect.Value) bool {
	zero := reflect.Zero(v.Type()).Interface()
	return reflect.DeepEqual(v.Interface(), zero)
}
