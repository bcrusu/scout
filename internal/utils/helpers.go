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
