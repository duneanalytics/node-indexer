package duneapi_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/duneanalytics/blockchain-ingester/client/duneapi"
	"github.com/duneanalytics/blockchain-ingester/models"
	"github.com/stretchr/testify/require"
)

func TestWriteBlockBatch(t *testing.T) {
	tests := []struct {
		name     string
		payloads []models.RPCBlock
		expected string
	}{
		{
			name: "single payload",
			payloads: []models.RPCBlock{
				{Payload: []byte(`{"block":1}`)},
			},
			expected: `{"block_sizes":[11]}
{"block":1}`,
		},
		{
			name: "multiple payloads, with new lines",
			payloads: []models.RPCBlock{
				{Payload: []byte(`{"block":1}` + "\n")},
				{Payload: []byte(`{"block":2}` + "\n")},
			},
			expected: `{"block_sizes":[12,12]}
{"block":1}
{"block":2}
`,
		},
		{
			name: "multiple payloads, no newlines",
			payloads: []models.RPCBlock{
				{Payload: []byte(`{"block":1}`)},
				{Payload: []byte(`{"block":2}`)},
			},
			expected: `{"block_sizes":[11,11]}
{"block":1}{"block":2}`,
		},
		{
			name:     "empty payloads",
			payloads: []models.RPCBlock{},
			expected: `{"block_sizes":[]}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := duneapi.WriteBlockBatch(&buf, tt.payloads, false)
			require.NoError(t, err)

			require.Equal(t, tt.expected, buf.String())
			rebuilt, err := ReadBlockBatch(&buf)
			require.NoError(t, err)
			require.EqualValues(t, tt.payloads, rebuilt)
		})
	}
}

func ReadBlockBatch(buf *bytes.Buffer) ([]models.RPCBlock, error) {
	reader := bufio.NewReader(buf)
	headerLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	var header duneapi.BlockBatchHeader
	err = json.Unmarshal([]byte(headerLine), &header)
	if err != nil {
		return nil, err
	}

	payloads := make([]models.RPCBlock, len(header.BlockSizes))
	for i, size := range header.BlockSizes {
		payload := make([]byte, size)
		_, err := io.ReadFull(reader, payload)
		if err != nil {
			return nil, err
		}
		payloads[i] = models.RPCBlock{Payload: payload}
	}

	return payloads, nil
}
