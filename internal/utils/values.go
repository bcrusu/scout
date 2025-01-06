package utils

import (
	"reflect"
	"strings"

	"github.com/bcrusu/scout/internal/errors"
)

// SetValues sets struct field values as specified via the values map.
func SetValues[T any](instance *T, values map[string]any) error {
	val := reflect.ValueOf(instance).Elem()
	typ := val.Type()
	if typ.Kind() != reflect.Struct {
		return errors.Error("input is not a struct")
	}

	clone := CloneMap(values)
	if err := setValuesRec(val, clone, ""); err != nil {
		return err
	}

	if len(clone) != 0 {
		unknown := strings.Join(MakeKeySlice(clone), ",")
		return errors.Errorf("values contains unknown fields: %s", unknown)
	}

	return nil
}

func setValuesRec(val reflect.Value, values map[string]any, parentName string) error {
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		name := field.Name
		if parentName != "" {
			name = parentName + "." + field.Name
		}

		if field.Type.Kind() == reflect.Struct {
			if err := setValuesRec(val.Field(i), values, name); err != nil {
				return err
			}
			continue
		}

		value, ok := values[name]
		if !ok {
			continue
		}

		fieldVal := val.Field(i)
		if !fieldVal.CanSet() {
			return errors.Errorf("field %s is read-only", name)
		}

		fieldType := field.Type
		valueType := reflect.TypeOf(value)
		if valueType != fieldType {
			return errors.Errorf("field %s type %s does not match the provided value type %s", name, fieldType, valueType)
		}

		fieldVal.Set(reflect.ValueOf(value))
		delete(values, name)
	}

	return nil
}
