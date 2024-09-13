module github.com/bcrusu/graph

go 1.23.0

replace github.com/Jille/raft-grpc-transport => ../multiraft-grpc-transport

require (
	github.com/Jille/raft-grpc-transport v1.6.1
	github.com/dustin/go-humanize v1.0.1
	github.com/google/uuid v1.6.0
	github.com/hashicorp/go-hclog v1.6.2
	github.com/hashicorp/raft v1.7.0
	github.com/onsi/ginkgo v1.16.5
	github.com/onsi/gomega v1.34.2
	github.com/spf13/cobra v1.8.1
	go.uber.org/mock v0.4.0
	golang.org/x/time v0.6.0
	google.golang.org/grpc v1.66.0
	google.golang.org/protobuf v1.34.2
)

require (
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.1 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/net v0.28.0 // indirect
	golang.org/x/sys v0.24.0 // indirect
	golang.org/x/text v0.17.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240827150818-7e3bb234dfed // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
