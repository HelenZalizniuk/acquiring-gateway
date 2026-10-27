PROTO_DIR := api/proto
GEN_DIR   := gen/go

PROTO_FILES := $(shell find $(PROTO_DIR) -name '*.proto')
export PATH := $(HOME)/.local/bin:$(shell go env GOPATH)/bin:$(PATH)

.PHONY: proto proto-tools proto-clean

## Install protoc plugins into GOPATH/bin
proto-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

## Generate Go + gRPC stubs from api/proto
proto: proto-tools
	@command -v protoc >/dev/null || { \
		echo "error: protoc not found. Install protobuf compiler:"; \
		echo "  Ubuntu/WSL: sudo apt-get install -y protobuf-compiler"; \
		echo "  macOS:      brew install protobuf"; \
		exit 1; \
	}
	@mkdir -p $(GEN_DIR)
	protoc \
		-I $(PROTO_DIR) \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)
	@echo "generated → $(GEN_DIR)"

proto-clean:
	rm -rf $(GEN_DIR)
