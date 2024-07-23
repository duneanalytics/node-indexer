package duneapi

import (
	"encoding/json"
	"io"

	"github.com/duneanalytics/blockchain-ingester/models"
)

type BlockBatchHeader struct {
	BlockSizes []int `json:"block_sizes"`
}

func WriteBlockBatch(out io.Writer, payloads []models.RPCBlock, disableHeader bool) error {
	// we write a batch header (single line, NDJSON) with the size of each block payload and then concatenate the payloads
	header := BlockBatchHeader{
		BlockSizes: make([]int, len(payloads)),
	}
	for i, block := range payloads {
		header.BlockSizes[i] = len(block.Payload)
	}
	// allow disabling the header for testing/backwards compatibility
	if !disableHeader {
		buf, err := json.Marshal(header)
		if err != nil {
			return err
		}
		_, err = out.Write(buf)
		if err != nil {
			return err
		}
		_, err = out.Write([]byte("\n"))
		if err != nil {
			return err
		}
	}
	for _, block := range payloads {
		_, err := out.Write(block.Payload)
		if err != nil {
			return err
		}
	}
	return nil
}
