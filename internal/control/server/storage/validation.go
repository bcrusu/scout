package storage

const (
	maxClusterNameLen = 100
	maxAddressLen     = 128
	maxTokenLen       = 1024
	maxPartitionCount = 1 << 16
)

func IsValidClusterName(value string) bool {
	return len(value) > 0 && len(value) <= maxClusterNameLen
}

func IsValidAddress(value string) bool {
	return len(value) > 0 && len(value) <= maxAddressLen
}

func IsValidToken(value string) bool {
	return len(value) > 0 && len(value) <= maxTokenLen
}

func IsValidPartitionCount(value uint32) bool {
	return value > 0 && value <= maxClusterNameLen
}
