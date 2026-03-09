.PHONY: all setup lint lint-timezone build test image-build image-push

TEST_TIMEOUT := 10s
SHELL := /bin/bash
TAG ?= latest

all: lint test build

setup: bin/golangci-lint bin/gofumpt bin/moq
	go mod download

bin:
	mkdir -p bin

bin/moq: bin
	GOBIN=$(PWD)/bin go install github.com/matryer/moq@v0.3.4
bin/golangci-lint: bin
	GOBIN=$(PWD)/bin go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.60.1
bin/gofumpt: bin
	GOBIN=$(PWD)/bin go install mvdan.cc/gofumpt@v0.6.0

build: cmd/main.go
	CGO_ENABLED=0 go build -ldflags="-X github.com/duneanalytics/blockchain-ingester/client/duneapi.commitHash=$(shell git rev-parse --short HEAD)" -o indexer cmd/main.go

lint: bin/golangci-lint bin/gofumpt lint-timezone
	go fmt ./...
	go vet ./...
	bin/golangci-lint -c .golangci.yml run ./...
	bin/gofumpt -l -e -d ./
	go mod tidy

# All binary entrypoints must set time.Local = time.UTC in init() to ensure
# time.Now() always returns UTC.
lint-timezone:
	@fail=0; \
	for f in $$(find . -name main.go -path '*/cmd/*/main.go'); do \
		if ! grep -q 'time\.Local = time\.UTC' "$$f"; then \
			echo "ERROR: $$f missing 'time.Local = time.UTC' in init()"; \
			fail=1; \
		fi; \
	done; \
	if [ "$$fail" -eq 1 ]; then exit 1; fi

test:
	go mod tidy
	CGO_ENABLED=1 go test -timeout=$(TEST_TIMEOUT) -race -bench=. -benchmem -cover ./...

gen-mocks: bin/moq ./client/jsonrpc/ ./client/duneapi/
	./bin/moq -pkg jsonrpc_mock -out ./mocks/jsonrpc/httpclient.go ./client/jsonrpc HTTPClient
	./bin/moq -pkg jsonrpc_mock -out ./mocks/jsonrpc/rpcnode.go ./client/jsonrpc BlockchainClient
	./bin/moq -pkg duneapi_mock -out ./mocks/duneapi/client.go ./client/duneapi BlockchainIngester

image-build:
	@echo "# Building Docker images"
	docker buildx build --platform linux/amd64,linux/arm64,linux/arm/v8 -t "duneanalytics/node-indexer:${TAG}" -f Dockerfile .

image-push:
	@echo "# Pushing Docker images to Docker Hub (after building)"
	echo -n "${DOCKER_HUB_KEY}" | docker login --username duneanalytics --password-stdin
	docker buildx create --name mybuilder
	docker buildx use mybuilder
	docker buildx build --platform linux/amd64,linux/arm64,linux/arm/v8 -t "duneanalytics/node-indexer:${TAG}" -f Dockerfile --push .
