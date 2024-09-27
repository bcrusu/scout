package graph

//go:generate protoc --go_out=. --go_opt=paths=source_relative ./pkg/api/common.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./pkg/api/key_value.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./pkg/api/graph.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/api/admin.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/control/control.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/control/server/storage/storage.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/data/common.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/data/service.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/data/server/storage/storage.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/rpc/serviceconfig/serviceconfig.proto

//go:generate protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative ./pkg/api/key_value.proto
//go:generate protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative ./pkg/api/graph.proto
//go:generate protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative ./internal/api/admin.proto
//go:generate protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative ./internal/control/control.proto
//go:generate protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative ./internal/data/service.proto
