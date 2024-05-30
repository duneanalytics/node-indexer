.PHONY: all setup lint harvester test image-build image-push

APPLICATION := harvester
GITHUB_SHA ?= HEAD
REF_TAG := $(shell echo ${GITHUB_REF_NAME} | tr -cd '[:alnum:]')
IMAGE_TAG := ${ECR_REGISTRY}/${ECR_REPOSITORY}:${REF_TAG}-$(shell git rev-parse --short "${GITHUB_SHA}")-${GITHUB_RUN_NUMBER}
TEST_TIMEOUT := 10s

all: lint test harvester

setup: bin/golangci-lint bin/gofumpt
	go mod download

bin/golangci-lint:
	GOBIN=$(PWD)/bin go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.55.2
bin/gofumpt: bin
	GOBIN=$(PWD)/bin go install mvdan.cc/gofumpt@v0.6.0

harvester: lint cmd/main.go
	go build -o harvester cmd/main.go

lint: bin/golangci-lint bin/gofumpt
	go fmt ./...
	go vet ./...
	bin/golangci-lint -c .golangci.yml run ./...
	bin/gofumpt -l -e -d ./
	go mod tidy

test:
	go mod tidy
	go test -timeout=$(TEST_TIMEOUT) -race -bench=. -benchmem -cover ./...

image-build:
	@echo "# Building harvester docker image..."
	docker build -t $(APPLICATION) -f Dockerfile --build-arg GITHUB_TOKEN=${GITHUB_TOKEN} .

image-push: image-build
	@echo "# Pushing harvester docker image..."
	docker tag $(APPLICATION) ${IMAGE_TAG}
	docker push ${IMAGE_TAG}
	docker rmi ${IMAGE_TAG}
