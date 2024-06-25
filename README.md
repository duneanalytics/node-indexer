# blockchain node indexer
A program that indexes blockchain data into HTTPS://DUNE.COM by connecting directly to an RPC node

# Limitations

This program works with EVM compatible blockchains, doing direct, EVM-specific JSON-RPC calls to the Node RPC endpoint.


# How to use:

There are only 3 required arguments for running the indexer:
  1. DUNE_API_KEY: Your Dune API Key, you can get this at: https://dune.com/settings/api
  1. BLOCKCHAIN_NAME: The name of the blockchain as configured on Dune (for example: "ethereum" blockchain)
  1. RPC_NODE_URL: The URL of the NODE RPC endpoint, for example: https://sepolia.optimism.io/

For more details see the configuration options section


## Docker Container

You can use our public docker container image and run it as such:

```bash
docker run -e BLOCKCHAIN_NAME='foo' -e RPC_NODE_URL='http://localhost:8545' -e DUNE_API_KEY='your-key-here' duneanalytics/node-indexer
```



## Binary executable

You can also just build and run a binary executable after cloning this repository:

Build the binary for your OS:
```bash
$ make build

$ BLOCKCHAIN_NAME='foo' RPC_NODE_URL='http://localhost:8545' DUNE_API_KEY='your-key-here' ./indexer
```

## Configuration Options

You can see all the configuration options by using the `--help` argument:
```bash
docker run duneanalytics/node-indexer ./indexer --help
```
