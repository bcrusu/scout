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

// GetOptionalParameter is used by variadic functions to specify a single optional parameter.
func GetOptionalParameter[T any](defaultValue T, values []T) T {
	if len(values) == 1 {
		return values[0]
	} else if len(values) > 1 {
		panic("expected a single value")
	}
	return defaultValue
}
