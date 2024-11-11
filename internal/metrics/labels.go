package metrics

import (
	"fmt"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
)

type Labels struct {
	attrSet attribute.Set
}

func NewLabels(args ...any) Labels {
	kvs := makeKeyValues(args)
	return Labels{attribute.NewSet(kvs...)}
}

func (l *Labels) With(args ...any) Labels {
	kvs := append(l.attrSet.ToSlice(), makeKeyValues(args)...)
	return Labels{attribute.NewSet(kvs...)}
}

func makeKeyValues(args []any) []attribute.KeyValue {
	if len(args)%2 != 0 {
		log.Warn("Invalid args len: %s", args)
		args = args[:len(args)-1]
	}

	attrs := make([]attribute.KeyValue, 0, len(args)/2)

	for i := 0; i < len(args)-1; i += 2 {
		key, ok := args[i].(string)
		if !ok {
			log.Warnf("Invalid arg key at index %d: %s", i, args)
			key = fmt.Sprintf("%s", args[i])
		}

		switch x := args[i+1].(type) {
		case bool:
			attrs = append(attrs, attribute.Bool(key, x))
		case int:
			attrs = append(attrs, attribute.Int(key, x))
		case int32:
			attrs = append(attrs, attribute.Int(key, int(x)))
		case int64:
			attrs = append(attrs, attribute.Int64(key, x))
		case uint32:
			attrs = append(attrs, attribute.String(key, strconv.FormatUint(uint64(x), 10)))
		case uint64:
			attrs = append(attrs, attribute.String(key, strconv.FormatUint(x, 10)))
		case float64:
			attrs = append(attrs, attribute.Float64(key, x))
		case string:
			attrs = append(attrs, attribute.String(key, x))
		case fmt.Stringer:
			attrs = append(attrs, attribute.Stringer(key, x))
		default:
			log.Warnf("Unhandled value type %T", x)
			attrs = append(attrs, attribute.String(key, fmt.Sprintf("%v", x)))
		}
	}

	return attrs
}

func mergeLabels(labels []Labels) attribute.Set {
	if len(labels) == 0 {
		return *attribute.EmptySet()
	} else if len(labels) == 1 {
		return labels[0].attrSet
	}

	count := 0
	for _, l := range labels {
		count += l.attrSet.Len()
	}

	kvs := make([]attribute.KeyValue, 0, count)
	for _, l := range labels {
		kvs = append(kvs, l.attrSet.ToSlice()...)
	}

	return attribute.NewSet(kvs...)
}
