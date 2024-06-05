FROM golang:1.22-bookworm AS builder

RUN apt update && apt install -y curl make

ARG GITHUB_TOKEN

# first copy just enough to pull all dependencies, to cache this layer
COPY go.mod go.sum Makefile /app/
WORKDIR /app/
RUN git config --global url."https://dune-eng:${GITHUB_TOKEN}@github.com".insteadOf "https://github.com" \
	&& make setup

# lint, build, etc..
COPY . /app/
RUN make build

FROM debian:bookworm-slim
RUN apt update \
	&& apt install -y ca-certificates \
	&& apt clean \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/ingester /app/
ENTRYPOINT ["/app/ingester"]
