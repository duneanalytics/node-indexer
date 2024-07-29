package jsonrpc_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/duneanalytics/blockchain-ingester/models"
	"github.com/stretchr/testify/require"
)

func TestOpStackBasic(t *testing.T) {
	getBlockByNumberResponse := readFileForTest(
		"testdata/opstack-MODE-block-0x7a549b-eth_getBlockByNumber.json")
	getBlockReceiptsResponse := readFileForTest(
		"testdata/opstack-MODE-block-0x7a549b-eth_getBlockReceipts.json")
	debugtraceBlockByNumberResponse := readFileForTest(
		"testdata/opstack-MODE-block-0x7a549b-debug_traceBlockByNumber.json")

	var expectedPayload bytes.Buffer
	expectedPayload.Write(getBlockByNumberResponse.Bytes())
	expectedPayload.Write(getBlockReceiptsResponse.Bytes())
	expectedPayload.Write(debugtraceBlockByNumberResponse.Bytes())
	expectedPayloadBytes := expectedPayload.Bytes()

	blockNumberHex := "0x7a549b"
	blockNumber := int64(8017051)
	httpClientMock := MockHTTPRequests(
		[]MockedRequest{
			{
				Req: jsonRPCRequest{
					Method: "eth_getBlockByNumber",
					Params: []interface{}{blockNumberHex, true},
				},
				Resp: jsonRPCResponse{
					Body: getBlockByNumberResponse,
				},
			},
			{
				Req: jsonRPCRequest{
					Method: "eth_getBlockReceipts",
					Params: []interface{}{blockNumberHex},
				},
				Resp: jsonRPCResponse{
					Body: getBlockReceiptsResponse,
				},
			},
			{
				Req: jsonRPCRequest{
					Method: "debug_traceBlockByNumber",
					Params: []interface{}{blockNumberHex, map[string]string{"tracer": "callTracer"}},
				},
				Resp: jsonRPCResponse{
					Body: debugtraceBlockByNumberResponse,
				},
			},
		})

	opstack, err := NewTestRPCClient(httpClientMock, models.OpStack)
	require.NoError(t, err)

	block, err := opstack.BlockByNumber(context.Background(), blockNumber)
	require.NoError(t, err)
	require.NotNil(t, block)
	require.Equal(t, blockNumber, block.BlockNumber)
	require.False(t, block.Errored())
	require.Equal(t, expectedPayloadBytes, block.Payload)
}
