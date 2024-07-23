package duneapi

import (
	"errors"
	"net/url"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var metricSendBlockCount = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "node_indexer",
		Subsystem: "dune_client",
		Name:      "sent_block_total",
		Help:      "Total number of blocks sent in requests",
	},
	[]string{"status"},
)

var metricSendBlockBatchSize = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "node_indexer",
		Subsystem: "dune_client",
		Name:      "block_per_batch",
		Help:      "Number of blocks per batch",
		Buckets:   []float64{1, 2, 4, 8, 16, 32, 64, 128, 256},
	},
	[]string{"status"},
)

var metricSendRequestsCount = promauto.NewCounterVec(

	prometheus.CounterOpts{
		Namespace: "node_indexer",
		Subsystem: "dune_client",
		Name:      "send_requests_total",
		Help:      "Number of send requests",
	},
	[]string{"status"},
)

var metricSendBlockBatchDurationMillis = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "node_indexer",
		Subsystem: "dune_client",
		Name:      "send_block_batch_duration_millis",
		Help:      "Duration of a send block batch request in milliseconds",
		Buckets:   []float64{10, 25, 50, 100, 250, 500, 1000, 2000, 4000},
	},
	[]string{"status"},
)

func observeSendBlocksRequest(status string, numberOfBlocks int, t0 time.Time) {
	metricSendBlockCount.WithLabelValues(status).Inc()
	metricSendBlockBatchSize.WithLabelValues(status).Observe(float64(numberOfBlocks))
	metricSendBlockBatchDurationMillis.WithLabelValues(status).Observe(float64(time.Since(t0).Milliseconds()))
	metricSendRequestsCount.WithLabelValues(status).Add(float64(numberOfBlocks))
}

func observeSendBlocksRequestCode(statusCode int, numberOfBlocks int, t0 time.Time) {
	observeSendBlocksRequest(strconv.Itoa(statusCode), numberOfBlocks, t0)
}

func observeSendBlocksRequestErr(err error, numberOfBlocks int, t0 time.Time) {
	observeSendBlocksRequest(errorToStatus(err), numberOfBlocks, t0)
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
