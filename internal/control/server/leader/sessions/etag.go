package sessions

import (
	"crypto/sha256"
	"encoding/base64"
	"slices"
)

func makeETag(values ...string) string {
	hash := sha256.New()
	slices.Sort(values)

	for _, value := range values {
		hash.Write([]byte(value))
	}

	bytes := make([]byte, 0, hash.Size())
	bytes = hash.Sum(bytes)
	return base64.RawURLEncoding.EncodeToString(bytes)
}
