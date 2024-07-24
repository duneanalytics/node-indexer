package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/duneanalytics/blockchain-ingester/lib/hexutils"
	"github.com/duneanalytics/blockchain-ingester/models"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/panjf2000/ants/v2"
)

type BlockchainClient interface {
	LatestBlockNumber() (int64, error)
	BlockByNumber(ctx context.Context, blockNumber int64) (models.RPCBlock, error)
	Close() error
}

const (
	MaxRetries               = 10
	DefaultRequestTimeout    = 30 * time.Second
	DefaultMaxRPCConcurrency = 50 // safe default
)

type rpcClient struct {
	client      *retryablehttp.Client
	cfg         Config
	log         *slog.Logger
	bufPool     *sync.Pool
	httpHeaders map[string]string
	wrkPool     *ants.Pool
}

func NewClient(logger *slog.Logger, cfg Config) (BlockchainClient, error) {
	switch cfg.EVMStack {
	case models.OpStack:
		return NewOpStackClient(logger, cfg)
	case models.ArbitrumNitro:
		return NewArbitrumNitroClient(logger, cfg)
	default:
		return nil, fmt.Errorf("unsupported EVM stack: %s", cfg.EVMStack)
	}
}

func newClient(log *slog.Logger, cfg Config) (*rpcClient, error) { // revive:disable-line:unexported-return
	client := retryablehttp.NewClient()
	client.RetryMax = MaxRetries
	client.Logger = log
	checkRetry := func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		yes, err2 := retryablehttp.DefaultRetryPolicy(ctx, resp, err)
		if yes {
			if resp == nil {
				log.Warn("Retrying request to RPC client", "error", err2)
			} else {
				log.Warn("Retrying request to RPC client", "statusCode", resp.Status, "error", err2)
			}
		}
		return yes, err2
	}
	client.CheckRetry = checkRetry
	client.Backoff = retryablehttp.LinearJitterBackoff
	client.HTTPClient.Timeout = DefaultRequestTimeout

	if cfg.TotalRPCConcurrency == 0 {
		cfg.TotalRPCConcurrency = DefaultMaxRPCConcurrency
	}
	wkrPool, err := ants.NewPool(cfg.TotalRPCConcurrency)
	if err != nil {
		return nil, fmt.Errorf("failed to create worker pool: %w", err)
	}

	rpc := &rpcClient{
		client: client,
		cfg:    cfg,
		log:    log,
		bufPool: &sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
		httpHeaders: cfg.HTTPHeaders,
		wrkPool:     wkrPool,
	}
	// Ensure RPC node is up & reachable
	_, err = rpc.LatestBlockNumber()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to jsonrpc: %w", err)
	}
	log.Info("Initialized and Connected to node jsonRPC", "config", fmt.Sprintf("%+v", cfg))
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
	ctx context.Context, method string, params []interface{}, output *bytes.Buffer,
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
	if c.httpHeaders != nil {
		for k, v := range c.httpHeaders {
			req.Header.Set(k, v)
		}
	}

	t0 := time.Now()
	resp, err := c.client.Do(req)
	if err != nil {
		observeRPCRequestErr(err, method, t0)
		return fmt.Errorf("failed to send request for method %s: %w", method, err)
	}
	defer resp.Body.Close()
	observeRPCRequestCode(resp.StatusCode, method, t0)

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
	c.wrkPool.Release()
	return nil
}

func (c *rpcClient) buildRPCBlockResponse(number int64, results []*bytes.Buffer) (models.RPCBlock, error) {
	var buffer bytes.Buffer
	for _, res := range results {
		buffer.Grow(res.Len())
		buffer.ReadFrom(res)
	}
	return models.RPCBlock{
		BlockNumber: number,
		Payload:     buffer.Bytes(),
	}, nil
}

func (c *rpcClient) putBuffer(buf *bytes.Buffer) {
	buf.Reset()
	c.bufPool.Put(buf)
}
