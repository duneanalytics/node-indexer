package jsonrpc

import (
	"errors"
	"net/url"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var rpcRequestCount = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "node_indexer",
		Subsystem: "rpc_client",
		Name:      "request_total",
		Help:      "Total number of RPC node requests",
	},
	[]string{"status", "method"},
)

var rpcRequestDurationMillis = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "node_indexer",
		Subsystem: "rpc_client",
		Name:      "request_duration_millis",
		Help:      "Duration of RPC node requests in milliseconds",
		Buckets:   []float64{10, 25, 50, 100, 250, 500, 1000, 2000, 4000},
	},
	[]string{"status", "method"},
)

func observeRPCRequest(status string, method string, t0 time.Time) {
	rpcRequestCount.WithLabelValues(status, method).Inc()
	rpcRequestDurationMillis.WithLabelValues(status, method).Observe(float64(time.Since(t0).Milliseconds()))
}

func observeRPCRequestCode(statusCode int, method string, t0 time.Time) {
	observeRPCRequest(strconv.Itoa(statusCode), method, t0)
}

func observeRPCRequestErr(err error, method string, t0 time.Time) {
	observeRPCRequest(errorToStatus(err), method, t0)
}

func errorToStatus(err error) string {
	status := "unknown_error"
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			status = "timeout"
		} else {
			status = "connection_refused"
		}
	}
	return status
}
