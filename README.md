# ScoutDB

ScoutDB is a distributed key-value database.

## Features

- Distributed
- Strongly consistent
- Two-phase commit transactions for multi-key operations
- Plugable storage backend (currently using [RocksDB](https://github.com/facebook/rocksdb/))
- Globally-consistent and causality-preserving snapshots enabled by:
  - MVCC storage
  - Hybrid Logical Clocks ([pdf](https://cse.buffalo.edu/tech-reports/2014-04.pdf))
- Time travel queries
- Automated replica rebalance and data streaming

## Building

### Docker

Simply run `make docker_images` to build everything as Docker images. The process will take a few moments as it needs to download and compile the latest version of RocksDB, but once completed the following images will be added to the local Docker repository:

- scout/control: the control plane server
- scout/data: the data plane server
- scout/api: the front-end API server
- scout/admin: the admin CLI tool

### Dev environment setup

Configure RocksDB:
- check the official [install guide](https://github.com/facebook/rocksdb/blob/master/INSTALL.md) for the supported platforms and build options
- download a recent [release](https://github.com/facebook/rocksdb/releases),
- the minimum supported version is 9.x.x as it contains specific changes made for ScoutDB (TODO)
- ensure that all dependencies listed in the install guide are available
- and compile it using `make static_lib` or `make shared_lib`
- or install using `make install-static` or `make install-shared`

Then configure the [grocksdb](https://github.com/linxGnu/grocksdb) package:
- set the CGO environment variables as [required](https://github.com/linxGnu/grocksdb/blob/master/README.md#install) 
- and don't forget to set the $LD_LIBRARY_PATH flag if you are building as shared lib with a custom build path.

And lastly, build ScoutDB components:
- `go build github.com/bcrusu/scout/cmd/control` to build the control plane server binary
- `go build github.com/bcrusu/scout/cmd/data` to build the data store binary
- `go build github.com/bcrusu/scout/cmd/api` builds the API binary
- `go build github.com/bcrusu/scout/cmd/admin` builds the admin command

For an example, check the [Dockerfile](Dockerfile) which builds RocksDB as static lib on Debian.

## Demo

The demo uses Docker Compose to bring up the following components:
- ScoutDB control and data planes
- ScoutDB API service
- Nginx proxy
- Observability stack: OpenTelemetry Collector, Grafana, Prometheus, and Jaeger
- the demo bridge network

To start the cluster:
- first build the Docker images as described above,
- then change to the *demo* dir, and
- run `make start` which creates by default a cluster with:
  - 3 control plane servers
  - 10 data plane servers
  - 3 api servers
  - and boostraps it with 100 partitions and replication factor 3
- later, when done run `make stop` to stop the containers and remove all persisted state

To interact with the cluster:
- run `go run demo.go` which starts writing and reading random data
- check [Grafana](http://localhost:3000/dashboards/f/scout/) dashboards to see the live cluster activity
- or use the admin command to query detailed info about the cluster:
  - `docker run -it --network scout_default scout/admin` to run the admin CLI container attached to the demo network
  - and from inside it run the `admin get` command to fetch details about the cluster, servers, partitions, and replicas:
  - for example, `admin get replicas --server control1:11001` lists all replicas and their state:
```
-----+------+---------+--------------+-------+-------+--------+------------------+----------------------+----------------------+----------------------+
|  #  | PART | REPLICA |    SERVER    | STATE | READY | LEADER | APPLIED/COMMITED |       CREATED        |      TRANSITION      |       UPDATED        |
+-----+------+---------+--------------+-------+-------+--------+------------------+----------------------+----------------------+----------------------+
|   1 |    0 | p0_r1   | data_11 (11) | Voter | ✓     | ✗      | 30/30            | 2024-11-18T12:00:42Z | 2024-11-18T12:00:42Z | 2024-11-18T12:02:31Z |
|   2 |    0 | p0_r2   | data_13 (13) | Voter | ✓     | ✗      | 30/30            | 2024-11-18T12:00:42Z | 2024-11-18T12:00:42Z | 2024-11-18T12:02:32Z |
|   3 |    0 | p0_r3   | data_6 (6)   | Voter | ✓     | TRUE   | 30/30            | 2024-11-18T12:00:42Z | 2024-11-18T12:00:42Z | 2024-11-18T12:02:33Z |
|   4 |    1 | p1_r1   | data_10 (10) | Voter | ✓     | ✗      | 23/25            | 2024-11-18T12:00:42Z | 2024-11-18T12:00:42Z | 2024-11-18T12:02:31Z |
|   5 |    1 | p1_r2   | data_12 (12) | Voter | ✓     | TRUE   | 25/25            | 2024-11-18T12:00:42Z | 2024-11-18T12:00:42Z | 2024-11-18T12:02:34Z |
|   6 |    1 | p1_r3   | data_9 (9)   | Voter | ✓     | ✗      | 25/25            | 2024-11-18T12:00:42Z | 2024-11-18T12:00:42Z | 2024-11-18T12:02:32Z |
|   7 |    2 | p2_r1   | data_7 (7)   | Voter | ✓     | ✗      | 23/23            | 2024-11-18T12:00:42Z | 2024-11-18T12:00:42Z | 2024-11-18T12:02:30Z |
|   8 |    2 | p2_r2   | data_8 (8)   | Voter | ✓     | ✗      | 23/23            | 2024-11-18T12:00:42Z | 2024-11-18T12:00:42Z | 2024-11-18T12:02:34Z |
|   9 |    2 | p2_r3   | data_11 (11) | Voter | ✓     | TRUE   | 23/23            | 2024-11-18T12:00:42Z | 2024-11-18T12:00:42Z | 2024-11-18T12:02:31Z |
|  10 |    3 | p3_r1   | data_6 (6)   | Voter | ✓     | ✗      | 21/21            | 2024-11-18T12:00:42Z | 2024-11-18T12:00:42Z | 2024-11-18T12:02:33Z |
...
```

To modify the cluster configuration, update:
- the [.env](demo/.env) file, and
- the config files in [scout](demo/scout) dir,
- then restart using `make restart` to apply the changes.

## Architecture

TODO: Add a nice diagram here

## Improvements

Essentially all system components can be improved in many ways, with some notable ideas listed below. In fact, it might be easier to rewrite the entire thing from scratch...

###  Raft optimizations

 - Goroutine count: the current implementation creates individual [Raft](https://github.com/hashicorp/raft) instances for each assigned replica with each instance spawning three persistent gorutines to handle the main protocol, the FSM apply loop, and taking FSM snapshots. In addition, other transient goroutines are created on a need-to basis to handle replication, voting during leader election, leadership transfer, emitting metrics, etc. A more optimal approach, described in [TiKV](https://tikv.org/deep-dive/scalability/multi-raft/), makes use of an event loop to drive all Raft instances in a batched manner. Moving to this approach would require some deep changes to the HashiCorp/Raft library internals, or completely switching to a different library like [etcd/Raft](https://github.com/etcd-io/raft) which only spawns a single goroutine per Raft instance and allows external control to advance the state machine ([link](https://github.com/etcd-io/raft/blob/93d0b5ceeb44b689f0037e8f946cb5433c1ac504/doc.go#L116)).
 
 - Heartbeat messages network traffic: even though the implementation uses a [gRPC-based transport](https://github.com/bcrusu/raft-grpc-transport/tree/multi) to multiplex RPCs from multiple Raft instances via a single TCP connection, the heartbeat messages are not coalesced at connection level, but rather handled separately for each Raft instance resulting in redundant messages. A nice visual explanation can be found in this CockroachDB [Scaling Raft](https://www.cockroachlabs.com/blog/scaling-raft/) blog post.

 - Switch the log storage from the [RocksDB-based store](https://github.com/bcrusu/raft-rocksdb) to a [WAL-based](https://github.com/hashicorp/raft-wal) approach. The main complexity lies in having all Raft instances write to a single WAL file in a similar way the [RocksDB single WAL](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-%28WAL%29) captures write logs for all column families. The alternative where each Raft instance maintains its separate WAL is trivial to configure, but sub-optimal when the number of Raft instances is large.

### Partitioning

- Better rebalance logic: the current implementation uses a naive card dealing replica assignement algorithm with a single objective to avoid multiple replicas for the same partition placed on the same node. More advanced solutions would provide support for user-defined policies to enforce replica placement across failure domains, take into account node capacity and current load, scheduled node maintenance operations, minimize data movement, and other synthetic metrics. Some good research avenues:
  - [Apache Helix CrushED](https://helix.apache.org/1.4.1-docs/design_crushed.html) employs a hybrid algorithm that uses [CRUSH](https://docs.ceph.com/en/reef/rados/operations/crush-map/), card dealing strategy, and consistent hashing to ensure both even distribution and minimal partition movement.
  - [Facebook Shard Manager](https://research.facebook.com/publications/shard-manager-a-generic-shard-management-framework-for-geo-distributed-applications/) uses a generic constraint solver with placement constraints and soft goals formulated as a constrained optimization problem.
  - [Google Slicer](https://research.google/pubs/slicer-auto-sharding-for-datacenter-applications/)'s weighted-move algorithm employs a greedy approach that makes the best move until the churn budget is exhausted.

- Dynamic partitioning: the current approach uses a static number of partitions configured during bootstrap which sets an upper limit on the total achievable storage capacity and does not account for data inbalance scenarios. Range-based partitioning would allow hot/cold ranges to be dynamically split/merged and at the same time handle interesting scaling scenarios like the the graph *supernodes* problem.

- Use [SST file ingestion](https://github.com/facebook/rocksdb/wiki/Creating-and-Ingesting-SST-files) for joining replicas: the current implementation uses an iterator to stream the RocksDB column family contents from a sponsor replica to the newly joining replica. This works just fine when the number of stored keys is small, but sub-optimal when the data size grows past a certain size. A better approach makes use of RocksDB [checkpointing](https://github.com/facebook/rocksdb/wiki/Checkpoints) feature to [export](https://github.com/facebook/rocksdb/blob/9a136e18b353e6d9c1b325103a4cef7d85a3ceea/include/rocksdb/utilities/checkpoint.h#L58) all column familiy SST files as hard-links to a specified directory from where they can be rsync-ed to the joining replica and ingested.

### Transactions/Storage

- Advance the snapshot read *safe* timestamp in absence of writes. Similar to the problem described in [Google Spanner](https://research.google/pubs/spanner-googles-globally-distributed-database-2/) paper, in section 4.2.4 Refinements, when a partition stops receiving write requests, the system needs to explicitly advance the *safe* timestamp to allow snapshot/replica reads past last written transaction timestamp. The simple fix would be to have the partition leader force and empty txn write after a timeout while the better approach involves a leader *lease* system as described in Spanner. A similar approach is also employed by CockroachDB [closed timestamps](https://www.cockroachlabs.com/docs/v24.2/architecture/transaction-layer#closed-timestamps) where the range leaseholder provides the promise to follower replicas to not accept any new writes below the closed timestamp which enables the [follower reads](https://www.cockroachlabs.com/docs/v24.2/follower-reads#how-stale-follower-reads-work) feature.

- RocksDB [compaction filter](https://github.com/facebook/rocksdb/wiki/Compaction-Filter): to prune old MVCC versions and enable a configurable historical data retention period.

- Support for interactive transactions: with client-driven multi read/write operations.

- Testing with [Jepsen](https://github.com/jepsen-io/jepsen): one of the main reasons I built the whole thing.

## License

What use has a license nowadays...
