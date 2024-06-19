FROM golang:1.22-alpine AS builder

# dependencies to build the project & dependencies
RUN apk add --no-cache git make curl gcc musl-dev binutils-gold bash

# First copy just enough to pull all dependencies, to cache this layer
COPY go.mod go.sum Makefile /app/
WORKDIR /app/
RUN make setup

# Copy the rest of the source code
COPY . .

# Build
RUN make build

# Stage 2: Create a minimal runtime image
FROM alpine:latest

# Install ca-certificates
RUN apk add --no-cache ca-certificates

COPY --from=builder /app/indexer /
ENTRYPOINT ["/indexer"]
