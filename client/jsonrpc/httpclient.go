package jsonrpc

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/hashicorp/go-retryablehttp"
)

type HTTPClient interface {
	Do(req *retryablehttp.Request) (*http.Response, error)
}

func NewHTTPClient(log *slog.Logger) *retryablehttp.Client {
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
	return client
}
