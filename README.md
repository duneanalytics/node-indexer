# Blockchain node indexer
A program that indexes blockchain data into http://dune.com by connecting directly to an RPC node.

# Limitations
This program works with EVM compatible blockchains, doing direct, EVM-specific JSON-RPC calls to the Node RPC endpoint.


# How to use:
There are only 3 required arguments for running the indexer:
  1. DUNE_API_KEY: Your Dune API Key, you can get this at: https://dune.com/settings/api
  1. BLOCKCHAIN_NAME: The name of the blockchain as configured on Dune (for example: "ethereum" blockchain)
  1. RPC_NODE_URL: The URL of the NODE RPC endpoint, for example: https://sepolia.optimism.io/

For more details see the configuration options section below.

## Docker container
You can run our [public container image on DockerHub](https://hub.docker.com/r/duneanalytics/node-indexer) as such:

```bash
docker run -e BLOCKCHAIN_NAME='foo' -e RPC_NODE_URL='http://localhost:8545' -e DUNE_API_KEY='your-key-here' duneanalytics/node-indexer
```

## Binary executable
You can also just build and run a binary executable after cloning this repository:

Build the binary for your OS:
```bash
$ make build

$ BLOCKCHAIN_NAME='foo' RPC_NODE_URL='http://localhost:8545' DUNE_API_KEY='your-key-here' LOG=debug ./indexer
```

Or run it directly with `go run`:
```bash
$ go run cmd/main.go --blockchain-name foo ...
```

## Configuration options
You can see all the configuration options by using the `--help` argument:
```bash
docker run duneanalytics/node-indexer --help
```

Also, we mention some of the options here:

### Log level
The `log` flag (environment variable `LOG`) controls the log level. Use `--log debug`/`LOG=debug` to emit more logs than the default `info` level. To emit less logs, use `warn`, or `error` (least).

### Tuning RPC concurrency
The flag `--rpc-concurrency` (environment variable `RPC_CONCURRENCY`) specifies the number of threads (goroutines)
to run concurrently to perform RPC node requests. Default is 25.

### RPC poll interval
The flag `--rpc-poll-interval` (environment variable `RPC_POLL_INTERVAL`) specifies the duration to wait before checking
if the RPC node has a new block. Default is `300ms`.

### Adding extra HTTP headers to RPC requests
If you wish to add HTTP headers to RPC requests you can do so by using the flag `--rpc-http-header` (once per header).

```
go run cmd/main.go ... --rpc-http-header header1:value1 --rpc-http-header header2:value2`
```

Or with the environment variable `RPC_HTTP_HEADERS='header1:value1|header2:value2|...'`, i.e. a `|` separated list of pairs,
where each pair is separated by `:` (make sure to quote the full string to avoid creating a pipe).

```
docker run --env RPC_HTTP_HEADERS='header1:value1|header2:value2' ... duneanalytics/node-indexer:<version>
```

## Metrics
The process exposes prometheus metrics which you can scrape and explore: `http://localhost:2112/metrics`
