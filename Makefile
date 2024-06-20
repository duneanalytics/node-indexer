.PHONY: all setup lint build test image-build image-push

TEST_TIMEOUT := 10s
SHELL := /bin/bash

all: lint test build

setup: bin/golangci-lint bin/gofumpt bin/moq
	go mod download

bin:
	mkdir -p bin

bin/moq: bin
	GOBIN=$(PWD)/bin go install github.com/matryer/moq@v0.3.4
bin/golangci-lint: bin
	GOBIN=$(PWD)/bin go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.59.1
bin/gofumpt: bin
	GOBIN=$(PWD)/bin go install mvdan.cc/gofumpt@v0.6.0

build: cmd/main.go
	CGO_ENABLED=0 go build -o indexer cmd/main.go

lint: bin/golangci-lint bin/gofumpt
	go fmt ./...
	go vet ./...
	bin/golangci-lint -c .golangci.yml run ./...
	bin/gofumpt -l -e -d ./
	go mod tidy

test:
	go mod tidy
	CGO_ENABLED=1 go test -timeout=$(TEST_TIMEOUT) -race -bench=. -benchmem -cover ./...

gen-mocks: bin/moq ./client/jsonrpc/ ./client/duneapi/
	./bin/moq -pkg jsonrpc_mock -out ./mocks/jsonrpc/rpcnode.go ./client/jsonrpc BlockchainClient
	./bin/moq -pkg duneapi_mock -out ./mocks/duneapi/client.go ./client/duneapi BlockchainIngester

image-build:
	@echo "# Building Docker images"
	docker buildx build --platform linux/amd64,linux/arm64,linux/arm/v8  -t public.ecr.aws/duneanalytics/node-indexer:latest -f Dockerfile .

image-push:
	@echo "# Pushing Docker images to ECR (after building)"
	docker buildx create --name mybuilder
	docker buildx use mybuilder
	docker buildx build --platform linux/amd64,linux/arm64,linux/arm/v8 -t public.ecr.aws/duneanalytics/node-indexer:latest -f Dockerfile --push .
