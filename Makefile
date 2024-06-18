.PHONY: all setup lint build test image-build image-push

GITHUB_SHA ?= HEAD
REF_TAG := $(shell echo ${GITHUB_REF_NAME} | tr -cd '[:alnum:]')
IMAGE_TAG := ${ECR_REGISTRY}/${ECR_REPOSITORY}:${REF_TAG}-$(shell git rev-parse --short "${GITHUB_SHA}")-${GITHUB_RUN_NUMBER}
TEST_TIMEOUT := 10s

all: lint test build

setup: bin/golangci-lint bin/gofumpt bin/moq
	go mod download

bin/moq:
	GOBIN=$(PWD)/bin go install github.com/matryer/moq@v0.3.4
bin/golangci-lint:
	GOBIN=$(PWD)/bin go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.59.0
bin/gofumpt: bin
	GOBIN=$(PWD)/bin go install mvdan.cc/gofumpt@v0.6.0

build: lint cmd/main.go
	go build -o indexer cmd/main.go

lint: bin/golangci-lint bin/gofumpt
	go fmt ./...
	go vet ./...
	bin/golangci-lint -c .golangci.yml run ./...
	bin/gofumpt -l -e -d ./
	go mod tidy

test:
	go mod tidy
	go test -timeout=$(TEST_TIMEOUT) -race -bench=. -benchmem -cover ./...

gen-mocks: bin/moq ./client/jsonrpc/ ./client/duneapi/
	./bin/moq -pkg jsonrpc_mock -out ./mocks/jsonrpc/rpcnode.go ./client/jsonrpc BlockchainClient
	./bin/moq -pkg duneapi_mock -out ./mocks/duneapi/client.go ./client/duneapi BlockchainIngester


image-build:
	@echo "# Building indexer docker image for amd64 and arm64"
	docker buildx build --platform linux/amd64 -t ${IMAGE_TAG}-amd64 -f Dockerfile --build-arg GITHUB_TOKEN=${GITHUB_TOKEN} .
	docker buildx build --platform linux/arm64 -t ${IMAGE_TAG}-arm64 -f Dockerfile --build-arg GITHUB_TOKEN=${GITHUB_TOKEN} .

image-push: image-build
	@echo "# Pushing indexer docker images"
	# docker manifest create --insecure "${IMAGE_TAG}" "${IMAGE_TAG}-amd64"
	# docker manifest create -a "${IMAGE_TAG}" "${IMAGE_TAG}-arm64" --insecure
	# docker manifest push "${IMAGE_TAG}" --insecure
	# docker push "${IMAGE_TAG}-amd64"
	# docker push "${IMAGE_TAG}-arm64"
	# docker rmi "${IMAGE_TAG}-amd64"
	# docker rmi "${IMAGE_TAG}-arm64"
