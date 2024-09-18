package utils

import "math/rand/v2"

// ShuffleSlice is a generic helper for rand.Shuffle.
func ShuffleSlice[T any](slice []T) []T {
	rand.Shuffle(len(slice), func(i, j int) {
		slice[i], slice[j] = slice[j], slice[i]
	})
	return slice
}
