package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/duneanalytics/blockchain-ingester/lib/hexutils"
	"github.com/duneanalytics/blockchain-ingester/models"
	"github.com/hashicorp/go-retryablehttp"
)

type BlockchainClient interface {
	LatestBlockNumber() (int64, error)
	BlockByNumber(ctx context.Context, blockNumber int64) (models.RPCBlock, error)
	Close() error
}

const (
	MaxRetries = 10
)

type rpcClient struct {
	client  *retryablehttp.Client
	cfg     Config
	log     *slog.Logger
	bufPool *sync.Pool
}

func NewClient(log *slog.Logger, cfg Config) (*rpcClient, error) { // revive:disable-line:unexported-return
	client := retryablehttp.NewClient()
	client.RetryMax = MaxRetries
	client.Logger = log
	client.CheckRetry = retryablehttp.DefaultRetryPolicy
	client.Backoff = retryablehttp.LinearJitterBackoff
	rpc := &rpcClient{
		client: client,
		cfg:    cfg,
		log:    log,
		bufPool: &sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
	// lets validate RPC node is up & reachable
	_, err := rpc.LatestBlockNumber()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to jsonrpc: %w", err)
	}
	return rpc, nil
}

func (c *rpcClient) LatestBlockNumber() (int64, error) {
	buf := c.bufPool.Get().(*bytes.Buffer)
	defer c.bufPool.Put(buf)
	buf.Reset()

	err := c.getResponseBody(context.Background(), "eth_blockNumber", []any{}, buf)
	if err != nil {
		c.log.Error("Failed to get response for jsonRPC",
			"method", "eth_blockNumber",
			"error", err,
		)
		return 0, err
	}
	resp := struct {
		Result string `json:"result"`
	}{}
	if err := json.NewDecoder(buf).Decode(&resp); err != nil {
		c.log.Error("Failed to decode response for jsonRPC", "error", err)
		return 0, err
	}
	return hexutils.IntFromHex(resp.Result)
}

// getResponseBody sends a request to the server and returns the response body
func (c *rpcClient) getResponseBody(
	ctx context.Context, method string, params interface{}, output *bytes.Buffer,
) error {
	reqData := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}
	encoder := json.NewEncoder(output)
	if err := encoder.Encode(reqData); err != nil {
		return err
	}
	req, err := retryablehttp.NewRequestWithContext(ctx, http.MethodPost, c.cfg.URL, output)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request for method %s: %w", method, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("response for method %s has status code %d", method, resp.StatusCode)
	}

	output.Reset()
	if _, err := output.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("failed to read response body for method %s: %w", method, err)
	}
	return nil
}

func (c *rpcClient) Close() error {
	return nil
}
