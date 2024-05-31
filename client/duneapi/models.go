package duneapi

import (
	"fmt"
)

type BlockchainIngestResponse struct {
	Tables []IngestedTableInfo `json:"tables"`
}

type IngestedTableInfo struct {
	Name  string `json:"name"`
	Rows  int    `json:"rows"`
	Bytes int    `json:"bytes"`
}

func (b *BlockchainIngestResponse) String() string {
	return fmt.Sprintf("Ingested: %+v", b.Tables)
}

type BlockchainIngestRequest struct {
	BlockNumber    string
	ContentType    string
	EVMStack       string
	IdempotencyKey string
	Payload        []byte
}
