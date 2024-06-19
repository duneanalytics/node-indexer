FROM golang:1.22-bookworm AS builder

RUN apt update && apt install -y curl make

# First copy just enough to pull all dependencies, to cache this layer
COPY go.mod go.sum Makefile /app/
WORKDIR /app/
RUN make setup

# Copy the rest of the source code
COPY . .

# Lint, build, etc..
COPY . /app/
RUN make test
RUN make build

FROM debian:bookworm-slim
RUN apt update \
	&& apt install -y ca-certificates \
	&& apt clean \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/indexer /
ENTRYPOINT ["/indexer"]
