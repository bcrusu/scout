package storage

type BootstrapResult struct {
	Success bool
}

type RegisterResult struct {
	ServerID   uint64
	ServerName string
}

type UpdateResult struct {
	NewVersion uint64
}
