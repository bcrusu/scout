package utils

// ContainsDuplicates returns true if the slice contains duplicate items.
func ContainsDuplicates[T comparable](slice []T) bool {
	m := map[T]bool{}

	for _, item := range slice {
		if m[item] {
			return true
		}

		m[item] = true
	}

	return false
}

// MakeSet returns a new set for the input slice.
func MakeSet[T comparable](slice []T) map[T]bool {
	result := map[T]bool{}
	for _, s := range slice {
		result[s] = true
	}
	return result
}

// MakeKeySlice returns map keys slice.
func MakeKeySlice[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// GetOptionalParameter is used by variadic functions to specify a single optional parameter.
func GetOptionalParameter[T any](defaultValue T, values []T) T {
	if len(values) == 1 {
		return values[0]
	} else if len(values) > 1 {
		panic("expected a single value")
	}
	return defaultValue
}

// PointerOf is a helper that returns the pointer of input value.
func PointerOf[T any](val T) *T {
	return &val
}

// GetSingleMapKey returns true if the map contains a single key.
func GetSingleMapKey[K comparable, V any](m map[K]V) (k K, v V, ok bool) {
	if len(m) == 1 {
		for k, v = range m {
			return k, v, true
		}
	}

	return k, v, false
}

// AppendMap appends source map items to dest map.
func AppendMap[K comparable, V any](dest, source map[K]V) {
	for k, v := range source {
		dest[k] = v
	}
}

func CloneMap[K comparable, V any](orig map[K]V) map[K]V {
	result := map[K]V{}
	for k, v := range orig {
		result[k] = v
	}
	return result
}
