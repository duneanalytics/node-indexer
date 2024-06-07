package duneapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/duneanalytics/blockchain-ingester/models"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/klauspost/compress/zstd"
)

const (
	MaxRetries = 20 // try really hard to send the block
)

type BlockchainIngester interface {
	// SendBlock sends a block to DuneAPI
	SendBlock(ctx context.Context, payload models.RPCBlock) error

	// - API to discover the latest block number ingested
	//   this can also provide "next block ranges" to push to DuneAPI
	// - log/metrics on catching up/falling behind, distance from tip of chain
}

type client struct {
	log        *slog.Logger
	httpClient *retryablehttp.Client
	cfg        Config
	compressor *zstd.Encoder
	bufPool    *sync.Pool
}

var _ BlockchainIngester = &client{}

func New(log *slog.Logger, cfg Config) (*client, error) { // revive:disable-line:unexported-return
	comp, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	httpClient := retryablehttp.NewClient()
	httpClient.RetryMax = MaxRetries
	httpClient.Logger = log
	httpClient.CheckRetry = retryablehttp.DefaultRetryPolicy
	httpClient.Backoff = retryablehttp.LinearJitterBackoff
	return &client{
		log:        log,
		httpClient: httpClient,
		cfg:        cfg,
		compressor: comp,
		bufPool: &sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}, nil
}

// SendBlock sends a block to DuneAPI
// TODO: support batching multiple blocks in a single request
func (c *client) SendBlock(ctx context.Context, payload models.RPCBlock) error {
	start := time.Now()
	buffer := c.bufPool.Get().(*bytes.Buffer)
	defer func() {
		c.bufPool.Put(buffer)
		c.log.Info("SendBlock", "payloadLength", len(payload.Payload), "duration", time.Since(start))
	}()

	request, err := c.buildRequest(payload, buffer)
	if err != nil {
		return err
	}
	return c.sendRequest(ctx, request)
}

func (c *client) buildRequest(payload models.RPCBlock, buffer *bytes.Buffer) (BlockchainIngestRequest, error) {
	var request BlockchainIngestRequest

	if c.cfg.DisableCompression {
		request.Payload = payload.Payload
		request.ContentType = "application/x-ndjson"
	} else {
		buffer.Reset()
		c.compressor.Reset(buffer)
		_, err := c.compressor.Write(payload.Payload)
		if err != nil {
			return request, err
		}
		request.ContentType = "application/zstd"
		request.Payload = buffer.Bytes()
	}
	request.BlockNumber = payload.BlockNumber
	request.IdempotencyKey = c.idempotencyKey(payload)
	request.EVMStack = c.cfg.Stack.String()
	return request, nil
}

func (c *client) sendRequest(ctx context.Context, request BlockchainIngestRequest) error {
	// TODO: implement timeouts (context with deadline)
	start := time.Now()
	var err error
	var response BlockchainIngestResponse
	var responseStatus string
	defer func() {
		if err != nil {
			c.log.Error("INGEST FAILED",
				"blockNumber", request.BlockNumber,
				"error", err,
				"statusCode", responseStatus,
				"duration", time.Since(start),
			)
		} else {
			c.log.Info("BLOCK INGESTED",
				"blockNumber", request.BlockNumber,
				"response", response.String(),
				"duration", time.Since(start),
			)
		}
	}()

	url := fmt.Sprintf("%s/beta/blockchain/%s/ingest", c.cfg.URL, c.cfg.BlockchainName)
	c.log.Debug("Sending request", "url", url)
	req, err := retryablehttp.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(request.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", request.ContentType)
	req.Header.Set("x-idempotency-key", request.IdempotencyKey)
	req.Header.Set("x-dune-evm-stack", request.EVMStack)
	req.Header.Set("x-dune-api-key", c.cfg.APIKey)
	req = req.WithContext(ctx)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	responseStatus = resp.Status
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return err
	}

	return nil
}

func (c *client) idempotencyKey(rpcBlock models.RPCBlock) string {
	// for idempotency we use the block number (should we use also the date?, or a startup timestamp?)
	return fmt.Sprintf("%v", rpcBlock.BlockNumber)
}

func (c *client) Close() error {
	return c.compressor.Close()
}
