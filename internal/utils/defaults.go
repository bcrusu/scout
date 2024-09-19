package utils

import (
	"reflect"
	"strconv"
	"time"

	"github.com/bcrusu/graph/internal/errors"
)

// SetDefaults sets struct field values as specified via the 'default' tag.
func SetDefaults[T any](instance *T) error {
	val := reflect.ValueOf(instance).Elem()
	typ := val.Type()
	if typ.Kind() != reflect.Struct {
		return errors.Error("input is not a struct")
	}

	return setDefaultsRec(val)
}

func setDefaultsRec(val reflect.Value) error {
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		if field.Type.Kind() == reflect.Struct {
			if err := setDefaultsRec(val.Field(i)); err != nil {
				return err
			}
		}

		defaultValue, ok := field.Tag.Lookup("default")
		if !ok {
			continue
		}

		fieldVal := val.Field(i)
		if !fieldVal.CanSet() || !fieldVal.IsZero() {
			continue
		}

		if err := setFieldDefault(fieldVal, defaultValue); err != nil {
			return errors.Wrapf(err, "failed to set default for field %s", field.Name)
		}
	}
	return nil
}

func setFieldDefault(val reflect.Value, defaultValue string) error {
	typeName := val.Type().String()
	switch typeName {
	case "time.Duration":
		v, err := time.ParseDuration(defaultValue)
		if err != nil {
			return err
		}
		val.Set(reflect.ValueOf(v))
	case "utils.Bytes":
		val.SetString(defaultValue)
	default:
		switch val.Kind() {
		case reflect.Bool:
			v, err := strconv.ParseBool(defaultValue)
			if err != nil {
				return err
			}
			val.SetBool(v)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			v, err := strconv.ParseInt(defaultValue, 10, 64)
			if err != nil {
				return err
			}
			val.SetInt(v)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			v, err := strconv.ParseUint(defaultValue, 10, 64)
			if err != nil {
				return err
			}
			val.SetUint(v)
		case reflect.String:
			val.SetString(defaultValue)
		case reflect.Float32, reflect.Float64:
			v, err := strconv.ParseFloat(defaultValue, 64)
			if err != nil {
				return err
			}
			val.SetFloat(v)
		default:
			return errors.Errorf("unsupported type %s", typeName)
		}
	}
	return nil
}
