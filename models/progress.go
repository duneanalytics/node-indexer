package models

import (
	"time"
)

type BlockchainIndexProgress struct {
	BlockchainName          string
	EVMStack                string
	LastIngestedBlockNumber int64
	LatestBlockNumber       int64
	Errors                  []BlockchainIndexError
}

type BlockchainIndexError struct {
	Timestamp    time.Time
	BlockNumbers string
	Error        string
	Source       string
}
