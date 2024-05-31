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

type Config struct {
	APIKey string
	URL    string

	// this is used by DuneAPI to determine the logic used to decode the EVM transactions
	Stack models.EVMStack
	// the name of this blockchain as it will be stored in DuneAPI
	BlockchainName string

	// RPC json payloads can be very large, we default to compressing for better throughput
	// - lowers latency
	// - reduces bandwidth
	DisableCompression bool
}

type RPCBlock struct {
	BlockNumber string
	Payload     []byte
}

type BlockchainIngester interface {
	// Sync pushes to DuneAPI the RPCBlockPayloads as they are received in an endless loop
	// it will block until:
	//	- the context is cancelled
	//  - channel is closed
	//  - a fatal error occurs
	Sync(ctx context.Context, blocksCh <-chan RPCBlock) error

	// SendBlock sends a block to DuneAPI
	SendBlock(payload RPCBlock) error

	// TODO:
	// - Batching multiple blocks in a single request
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
	return &client{
		log:        log,
		httpClient: retryablehttp.NewClient(),
		cfg:        cfg,
		compressor: comp,
		bufPool: &sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}, nil
}

func (c *client) Sync(ctx context.Context, blocksCh <-chan RPCBlock) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case payload, ok := <-blocksCh:
			if !ok {
				return nil // channel closed
			}
			if err := c.SendBlock(payload); err != nil {
				// TODO: implement DeadLetterQueue
				// this will leave a "block gap" in DuneAPI, TODO: implement a way to fill this gap
				c.log.Error("SendBlock failed, continuing..", "blockNumber", payload.BlockNumber, "error", err)
			}
		}
	}
}

// SendBlock sends a block to DuneAPI
func (c *client) SendBlock(payload RPCBlock) error {
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
	return c.sendRequest(request)
}

func (c *client) buildRequest(payload RPCBlock, buffer *bytes.Buffer) (BlockchainIngestRequest, error) {
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
	request.IdempotencyKey = c.idempotencyKey(payload)
	request.EVMStack = c.cfg.Stack.String()
	return request, nil
}

func (c *client) sendRequest(request BlockchainIngestRequest) error {
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

	url := fmt.Sprintf("%s/beta/blockchain/%s/chain", c.cfg.URL, c.cfg.BlockchainName)
	req, err := retryablehttp.NewRequest("POST", url, bytes.NewReader(request.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", request.ContentType)
	req.Header.Set("x-idempotency-key", request.IdempotencyKey)
	req.Header.Set("x-dune-evm-stack", request.EVMStack)
	req.Header.Set("x-dune-api-key", c.cfg.APIKey)

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

func (c *client) idempotencyKey(rpcBlock RPCBlock) string {
	// for idempotency we use the chain and block number
	return fmt.Sprintf("%s-%s", c.cfg.BlockchainName, rpcBlock.BlockNumber)
}

func (c *client) Close() error {
	return c.compressor.Close()
}
