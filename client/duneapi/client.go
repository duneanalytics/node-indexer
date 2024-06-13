package duneapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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

	// GetProgressReport gets a progress report from DuneAPI
	GetProgressReport(ctx context.Context) (*models.BlockchainIndexProgress, error)

	// PostProgressReport sends a progress report to DuneAPI
	PostProgressReport(ctx context.Context, progress models.BlockchainIndexProgress) error

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
		log:        log.With("module", "duneapi"),
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
func (c *client) SendBlock(ctx context.Context, payload models.RPCBlock) error {
	buffer := c.bufPool.Get().(*bytes.Buffer)
	defer c.bufPool.Put(buffer)

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
	} else {
		// not thread safe, multiple calls to the compressor here
		buffer.Reset()
		c.compressor.Reset(buffer)
		_, err := c.compressor.Write(payload.Payload)
		if err != nil {
			return request, err
		}
		c.compressor.Close()
		request.ContentEncoding = "application/zstd"
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
				"payloadSize", len(request.Payload),
				"duration", time.Since(start),
			)
		} else {
			c.log.Info("BLOCK SENT",
				"blockNumber", request.BlockNumber,
				"response", response.String(),
				"payloadSize", len(request.Payload),
				"duration", time.Since(start),
			)
		}
	}()

	url := fmt.Sprintf("%s/api/beta/blockchain/%s/ingest", c.cfg.URL, c.cfg.BlockchainName)
	c.log.Debug("Sending request", "url", url)
	req, err := retryablehttp.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(request.Payload))
	if err != nil {
		return err
	}
	if request.ContentEncoding != "" {
		req.Header.Set("Content-Encoding", request.ContentEncoding)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("x-idempotency-key", request.IdempotencyKey)
	req.Header.Set("x-dune-evm-stack", request.EVMStack)
	req.Header.Set("x-dune-api-key", c.cfg.APIKey)
	req = req.WithContext(ctx)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v, %v", resp.StatusCode, resp.Status)
	}
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

func (c *client) PostProgressReport(ctx context.Context, progress models.BlockchainIndexProgress) error {
	var request BlockchainProgress
	var err error
	var responseStatus string
	var responseBody string
	start := time.Now()

	// Log response
	defer func() {
		if err != nil {
			c.log.Error("Sending progress report failed",
				"lastIngestedBlockNumer", request.LastIngestedBlockNumber,
				"error", err,
				"statusCode", responseStatus,
				"duration", time.Since(start),
				"responseBody", responseBody,
			)
		} else {
			c.log.Info("Sent progress report",
				"lastIngestedBlockNumer", request.LastIngestedBlockNumber,
				"latestBlockNumber", request.LatestBlockNumber,
				"duration", time.Since(start),
			)
		}
	}()

	request = BlockchainProgress{
		LastIngestedBlockNumber: progress.LastIngestedBlockNumber,
		LatestBlockNumber:       progress.LatestBlockNumber,
	}
	url := fmt.Sprintf("%s/api/beta/blockchain/%s/ingest/progress", c.cfg.URL, c.cfg.BlockchainName)
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	c.log.Info("Sending request", "url", url, "payload", string(payload))
	req, err := retryablehttp.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-dune-api-key", c.cfg.APIKey)
	req = req.WithContext(ctx)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	responseStatus = resp.Status

	if resp.StatusCode != http.StatusOK {
		bs, err := io.ReadAll(resp.Body)
		responseBody = string(bs)
		if err != nil {
			return err
		}
		err = fmt.Errorf("got non-OK response, status code: %s body: %s", responseStatus, responseBody)
		return err
	}

	return nil
}

func (c *client) GetProgressReport(ctx context.Context) (*models.BlockchainIndexProgress, error) {
	var response BlockchainProgress
	var err error
	var responseStatus string
	start := time.Now()

	// Log response
	defer func() {
		if err != nil {
			c.log.Error("Getting progress report failed",
				"error", err,
				"statusCode", responseStatus,
				"duration", time.Since(start),
			)
		} else {
			c.log.Info("Got progress report",
				"progress", response.String(),
				"duration", time.Since(start),
			)
		}
	}()

	url := fmt.Sprintf("%s/api/beta/blockchain/%s/ingest/progress", c.cfg.URL, c.cfg.BlockchainName)
	c.log.Debug("Sending request", "url", url)
	req, err := retryablehttp.NewRequestWithContext(ctx, "GET", url, nil) // empty body
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-dune-api-key", c.cfg.APIKey)
	req = req.WithContext(ctx)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		var errorResp errorResponse
		err = json.Unmarshal(responseBody, &errorResp)
		if err != nil {
			return nil, err
		}
		err = fmt.Errorf("got non-OK response, status code: %d, body: '%s'", resp.StatusCode, errorResp.Error)
		// No progress yet
		if resp.StatusCode == http.StatusNotFound {
			return &models.BlockchainIndexProgress{
				BlockchainName:          c.cfg.BlockchainName,
				EVMStack:                c.cfg.Stack.String(),
				LastIngestedBlockNumber: 0,
				LatestBlockNumber:       0,
			}, nil
		}
		return nil, err
	}

	err = json.Unmarshal(responseBody, &response)
	if err != nil {
		return nil, err
	}

	progress := &models.BlockchainIndexProgress{
		BlockchainName:          c.cfg.BlockchainName,
		EVMStack:                c.cfg.Stack.String(),
		LastIngestedBlockNumber: response.LastIngestedBlockNumber,
		LatestBlockNumber:       response.LatestBlockNumber,
	}
	return progress, nil
}
