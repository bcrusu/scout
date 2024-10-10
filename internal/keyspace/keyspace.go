package keyspace

const (
	// ReservedDB is the reserved keyspace to be used by the backing DB implementation.
	// For example: RocksDB uses separate column families for each partition and each
	// is tagged with the partition id and last applied Raft index keys, both stored in
	// this keyspace. Other possible usecase is for storing runtime/storage stats, and
	// other implmentation-specific bits.
	ReservedDB uint32 = 0

	// ReservedReplica is reserved for replica lifecycle. For example, when a new partition
	// replica joins the replication group, it will stream the DB key-value contents from
	// an up-to-date sponsor replica/s and use this keyspace to save progress information,
	// like the last copied key, how much data was copied and how much is left, etc.
	// During normal/serving state, runtime stats could be persisted, etc.
	ReservedReplica uint32 = 1

	// All keyspaces up to and including this value are reserved for internal needs and are
	// not meant to be replicated (i.e. local to a single replica)
	ReservedLast uint32 = 15

	// All keyspaces starting here will be dynamically allocated to serve user requests, with
	// each keyspace dedicated to a specific user-database instance.
	FirstUserKeyspace uint32 = 16
)
