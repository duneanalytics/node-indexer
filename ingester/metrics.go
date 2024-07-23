package ingester

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func registerIngesterMetrics(i *ingester) {
	registerLatestBlockNumberGauge(func() int64 {
		return atomic.LoadInt64(&i.info.LatestBlockNumber)
	})
	registerIngestedBlockNumberGauge(func() int64 {
		return atomic.LoadInt64(&i.info.IngestedBlockNumber)
	})
	registerDlqSizeGauge(func() int {
		return i.dlq.Size()
	})
}

func registerLatestBlockNumberGauge(function func() int64) {
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "node_indexer",
		Name:      "latest_block_number",
		Help:      "The latest known block number for the chain",
	}, func() float64 {
		return float64(function())
	})
}

func registerIngestedBlockNumberGauge(function func() int64) {
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "node_indexer",
		Name:      "ingested_block_number",
		Help:      "The highest block number ingested so far",
	}, func() float64 {
		return float64(function())
	})
}

func registerDlqSizeGauge(function func() int) {
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "node_indexer",
		Name:      "dlq_size",
		Help:      "The number of blocks in the DLQ",
	}, func() float64 {
		return float64(function())
	})
}
