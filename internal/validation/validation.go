package validation

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/bcrusu/graph/internal/errors"
)

var (
	canValidateType = reflect.TypeFor[CanValidate]()
)

type CanValidate interface {
	Validate() error
}

// Validate checks struct field values as specified via the 'validate' tag
// and fields that implement the CanValidate interface.
func Validate(instance any) error {
	val := unwrap(reflect.ValueOf(instance))
	typ := val.Type()
	if typ.Kind() != reflect.Struct {
		return errors.Error("input is not a struct")
	}

	msgs, err := validateRec(val, "")
	if err != nil {
		return err
	}

	if len(msgs) > 0 {
		return errors.Errorf("validation failed: %s", strings.Join(msgs, ", "))
	}

	return nil
}

func validateRec(val reflect.Value, currPath string) ([]string, error) {
	typ := val.Type()
	var result []string

	addResult := func(field, msg string) {
		if msg == "" {
			return
		}

		path := ""
		if field != "" {
			if currPath != "" {
				path = currPath + "." + field
			} else {
				path = field
			}
		} else {
			if currPath != "" {
				path = currPath
			}
		}

		if path == "" {
			result = append(result, msg)
		} else {
			result = append(result, fmt.Sprintf("%s: %s", path, msg))
		}
	}

	if typ.Implements(canValidateType) {
		canValidate := val.Convert(canValidateType).Interface().(CanValidate)
		if err := canValidate.Validate(); err != nil {
			addResult("", err.Error())
		}
	}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldVal := val.Field(i)
		if !fieldVal.CanInterface() {
			continue
		}

		spec, hasSpec := field.Tag.Lookup("validate")
		if hasSpec && spec == "skip" {
			continue
		}

		if field.Type.Kind() == reflect.Struct {
			nextPath := field.Name
			if currPath != "" {
				nextPath = currPath + "." + field.Name
			}

			msgs, err := validateRec(fieldVal, nextPath)
			if err != nil {
				return nil, err
			}

			result = append(result, msgs...)
		}

		if !hasSpec {
			continue
		}

		validators, isRequired, err := parseValidateSpec(spec)
		if err != nil {
			addResult(field.Name, err.Error())
			continue
		}

		if isRequired {
			if msg := required(fieldVal); msg != "" {
				addResult(field.Name, msg)
				continue
			}
		}

		for _, v := range validators {
			addResult(field.Name, v.valFn(fieldVal, v.param))
		}
	}

	return result, nil
}

func parseValidateSpec(spec string) ([]validator, bool, error) {
	vals := strings.Split(spec, ",")
	var result []validator
	isRequired := false

	for _, val := range vals {
		val = strings.TrimSpace(val)
		if val == "" {
			continue
		}

		name := val
		params := ""
		if i := strings.Index(val, ":"); i >= 0 {
			name = val[:i]
			params = val[i+1:]
		}

		if name == "required" {
			isRequired = true
			continue
		}

		valFn, ok := allValidators[name]
		if !ok {
			return nil, false, errors.Errorf("unknown validator %s", name)
		}

		result = append(result, validator{
			valFn: valFn,
			param: params,
		})
	}

	return result, isRequired, nil
}

func unwrap(v reflect.Value) reflect.Value {
	k := v.Kind()
	if (k == reflect.Ptr || k == reflect.Interface) && !v.IsNil() {
		return v.Elem()
	}

	return v
}
