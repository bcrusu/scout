package graph

//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/api/api.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/control/control.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/control/server/storage/storage.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/data/data.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/data/server/storage/storage.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative ./internal/rpc/serviceconfig/serviceconfig.proto

//go:generate protoc --go-grpc_out=. ./internal/api/api.proto
//go:generate protoc --go-grpc_out=. ./internal/control/control.proto
//go:generate protoc --go-grpc_out=. ./internal/data/data.proto
