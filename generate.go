package scout

//go:generate protoc --go_out=. --go_opt=paths=source_relative ./pkg/keyvalue/key_value.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./pkg/graph/graph.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/api/api.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/control/control.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/control/server/storage/storage.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/data/data.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/data/server/storage/storage.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/rpc/serviceconfig/serviceconfig.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/testing/agent/agent.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/testing/nodes/nodes.proto

//go:generate protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative ./pkg/keyvalue/key_value.proto
//go:generate protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative ./pkg/graph/graph.proto
//go:generate protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative ./internal/api/api.proto
//go:generate protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative ./internal/control/control.proto
//go:generate protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative ./internal/data/data.proto
//go:generate protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative ./internal/testing/agent/agent.proto
//go:generate protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative ./internal/testing/nodes/nodes.proto
