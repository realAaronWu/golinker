# Golinker - Go RDMA RPC Library
# Build infrastructure with CGo support for RDMA libraries

MODULE := github.com/wua20/golinker

# CGo flags for RDMA libraries
export CGO_ENABLED := 1
CGO_LDFLAGS := -libverbs -lrdmacm -lnuma

# Build tags
MOCK_TAGS := mock
INTEGRATION_TAGS := integration

.PHONY: build test clean bench

# Build all packages with CGo enabled and RDMA library linking
build:
	CGO_ENABLED=1 CGO_LDFLAGS="$(CGO_LDFLAGS)" go build -tags $(MOCK_TAGS) ./...

# Run unit tests (mock mode, no real RDMA hardware required)
test:
	CGO_ENABLED=1 CGO_LDFLAGS="$(CGO_LDFLAGS)" go test -tags $(MOCK_TAGS) ./...

# Run integration tests (requires real RDMA hardware)
test-integration:
	CGO_ENABLED=1 CGO_LDFLAGS="$(CGO_LDFLAGS)" go test -tags $(INTEGRATION_TAGS) ./...

# Run benchmarks
bench:
	CGO_ENABLED=1 CGO_LDFLAGS="$(CGO_LDFLAGS)" go test -tags $(MOCK_TAGS) -bench=. -benchmem ./...

# Clean build artifacts
clean:
	go clean ./...
	rm -f bin/*
