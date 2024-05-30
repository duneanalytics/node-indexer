package duneapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/duneanalytics/blockchain-ingester/models"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/klauspost/compress/zstd"
)

type Config struct {
	URL                string
	APIKey             string
	Stack              models.EVMStack
	DisableCompression bool
	BlockchainName     string
}

type RPCBlockPayload struct {
	BlockNumber string
	Payload     []byte
}

type BlockchainIngester interface {
	// Sync pushes to DuneAPI the RPCBlockPayloads as they are received in an endless loop
	// it will block until:
	//	- the context is cancelled
	//  - channel is closed
	//  - a fatal error occurs
	Sync(ctx context.Context, blocksCh <-chan RPCBlockPayload) error
}

type client struct {
	log        slog.Logger
	httpClient *retryablehttp.Client
	cfg        Config
	compressor *zstd.Encoder
}

func New(log slog.Logger, cfg Config) (*client, error) {
	comp, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	return &client{
		log:        log,
		httpClient: retryablehttp.NewClient(),
		cfg:        cfg,
		compressor: comp,
	}, nil
}

func (c *client) Sync(ctx context.Context, blocksCh <-chan RPCBlockPayload) error {
	// use a long lived buffer to avoid memory allocations
	var buffer bytes.Buffer
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case payload, ok := <-blocksCh:
			if !ok {
				return nil // channel closed
			}
			request, err := c.buildRequest(&buffer, payload)
			if err != nil {
				return err
			}
			if err := c.sendBlock(request); err != nil {
				return err
			}
		}
	}
}

func (c *client) buildRequest(buffer *bytes.Buffer, payload RPCBlockPayload) (BlockchainIngestRequest, error) {
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

func (c *client) sendBlock(request BlockchainIngestRequest) error {
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
	req, err := http.NewRequest("POST", url, bytes.NewReader(request.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", request.ContentType)
	req.Header.Set("x-idempotency-key", request.IdempotencyKey)
	req.Header.Set("x-dune-evm-stack", request.EVMStack.String())
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

func (c *client) idempotencyKey(rpcBlock RPCBlockPayload) string {
	// for idempotency we use the chain and block number
	return fmt.Sprintf("%s-%s", c.cfg.BlockchainName, rpcBlock.BlockNumber)
}

func (c *client) Close() error {
	return c.compressor.Close()
}
