package jsonrpc_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/duneanalytics/blockchain-ingester/client/jsonrpc"
	jsonrpc_mock "github.com/duneanalytics/blockchain-ingester/mocks/jsonrpc"
	"github.com/duneanalytics/blockchain-ingester/models"
	"github.com/hashicorp/go-retryablehttp"
)

type jsonRPCRequest struct {
	Method      string        `json:"method"`
	Params      []interface{} `json:"params"`
	HTTPHeaders http.Header
}

type jsonRPCResponse struct {
	Body        io.Reader
	StatusCode  int    // optional, default to 200
	ContentType string // optional, default to "application/json"
}

type MockedRequest struct {
	Req  jsonRPCRequest
	Resp jsonRPCResponse
}

func MockHTTPRequests(requests []MockedRequest) *jsonrpc_mock.HTTPClientMock {
	// helper function to setup a mock http client with recorded request responses
	// non-registered requests will return an error
	return &jsonrpc_mock.HTTPClientMock{
		DoFunc: func(req *retryablehttp.Request) (*http.Response, error) {
			if req.Method != http.MethodPost {
				return nil, fmt.Errorf("expected POST method, got %s", req.Method)
			}
			// we use httpretryable.Client, so we can't use req.Body directly
			// we need to read the body and then reset it

			body, err := req.BodyBytes()
			if err != nil {
				return nil, err
			}
			var jsonReq jsonRPCRequest
			if err := json.Unmarshal(body, &jsonReq); err != nil {
				return nil, err
			}
			jsonReqParams := fmt.Sprintf("%+v", jsonReq.Params)
			// looking for a matching request
			for _, r := range requests {
				if r.Req.Method == jsonReq.Method {
					// we do this because reflect.DeepEquals() Comparison fails on map[string]any != map[string]string
					if jsonReqParams != fmt.Sprintf("%+v", r.Req.Params) {
						continue
					}
					// this is a match, validate registered headers
					for k, v := range r.Req.HTTPHeaders {
						if req.Header.Get(k) != v[0] {
							return nil, fmt.Errorf("expected header %s to be %s, got %s", k, v[0], req.Header.Get(k))
						}
					}
					// all headers match, return the response
					resp := &http.Response{
						StatusCode: 200,
						Body:       io.NopCloser(r.Resp.Body),
						Header:     make(http.Header),
					}
					if r.Resp.StatusCode != 0 {
						resp.StatusCode = r.Resp.StatusCode
					}
					resp.Header.Set("Content-Type", "application/json")
					if r.Resp.ContentType != "" {
						resp.Header.Set("Content-Type", r.Resp.ContentType)
					}
					return resp, nil
				}
			}
			// for simplificy, we include a default response for eth_blockNumber with a valid response
			if jsonReq.Method == "eth_blockNumber" {
				resp := &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x7a549b"}`))),
				}
				return resp, nil
			}
			return nil, fmt.Errorf("no matching request found, req: %+v", jsonReq)
		},
	}
}

func NewTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func NewTestRPCClient(httpClient jsonrpc.HTTPClient, stack models.EVMStack) (jsonrpc.BlockchainClient, error) {
	return jsonrpc.NewRPCClient(NewTestLogger(), httpClient, jsonrpc.Config{EVMStack: stack})
}

func readFileForTest(filename string) *bytes.Buffer {
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Panicf("Failed to read file: %v", err)
	}
	return bytes.NewBuffer(data)
}
