package models

type BlockchainIndexProgress struct {
	BlockchainName          string
	EVMStack                string
	LastIngestedBlockNumber int64
	LatestBlockNumber       int64
}
