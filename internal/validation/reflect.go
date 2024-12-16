package validation

import (
	"reflect"
)

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

func getString(v reflect.Value) (string, string) {
	if kind := v.Kind(); kind != reflect.String {
		return "", "invalid string field type"
	}

	return v.String(), ""
}
