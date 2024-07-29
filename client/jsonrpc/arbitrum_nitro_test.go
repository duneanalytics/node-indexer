package jsonrpc_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/duneanalytics/blockchain-ingester/models"
	"github.com/stretchr/testify/require"
)

func TestArbitrumNitroBasic(t *testing.T) {
	getBlockByNumberResponse := readFileForTest(
		"testdata/arbitrumnitro-DEGEN-block-0x16870e9-eth_getBlockByNumber.json")
	debugtraceBlockByNumberResponse := readFileForTest(
		"testdata/arbitrumnitro-DEGEN-block-0x16870e9-debug_traceBlockByNumber.json")
	tx0ReceiptResponse := readFileForTest(
		"testdata/arbitrumnitro-DEGEN-block-0x16870e9-eth_getTransactionReceipt-0x0.json")
	tx1ReceiptResponse := readFileForTest(
		"testdata/arbitrumnitro-DEGEN-block-0x16870e9-eth_getTransactionReceipt-0x1.json")

	var expectedPayload bytes.Buffer
	expectedPayload.Write(getBlockByNumberResponse.Bytes())
	expectedPayload.Write(debugtraceBlockByNumberResponse.Bytes())
	expectedPayload.Write(tx0ReceiptResponse.Bytes())
	expectedPayload.Write(tx1ReceiptResponse.Bytes())
	expectedPayloadBytes := expectedPayload.Bytes()

	tx0Hash := "0x19ee83020d4dad7e96dbb2c01ce2441e75717ee038a022fc6a3b61300b1b801c"
	tx1Hash := "0x4e805891b568698f8419f8e162d70ed9675e42a32e4972cbeb7f78d7fd51de76"
	blockNumberHex := "0x16870e9"
	blockNumber := int64(23621865)
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
					Method: "debug_traceBlockByNumber",
					Params: []interface{}{blockNumberHex, map[string]string{"tracer": "callTracer"}},
				},
				Resp: jsonRPCResponse{
					Body: debugtraceBlockByNumberResponse,
				},
			},
			{
				Req: jsonRPCRequest{
					Method: "eth_getTransactionReceipt",
					Params: []interface{}{tx0Hash},
				},
				Resp: jsonRPCResponse{
					Body: tx0ReceiptResponse,
				},
			},
			{
				Req: jsonRPCRequest{
					Method: "eth_getTransactionReceipt",
					Params: []interface{}{tx1Hash},
				},
				Resp: jsonRPCResponse{
					Body: tx1ReceiptResponse,
				},
			},
		})

	opstack, err := NewTestRPCClient(httpClientMock, models.ArbitrumNitro)
	require.NoError(t, err)

	block, err := opstack.BlockByNumber(context.Background(), blockNumber)
	require.NoError(t, err)
	require.NotNil(t, block)
	require.Equal(t, blockNumber, block.BlockNumber)
	require.False(t, block.Errored())
	require.Equal(t, expectedPayloadBytes, block.Payload)
}
