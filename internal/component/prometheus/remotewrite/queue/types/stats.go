package types

import (
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

type PrometheusStats struct {
	// Network Stats
	NetworkSeriesSent                prometheus.Counter
	NetworkFailures                  prometheus.Counter
	NetworkRetries                   prometheus.Counter
	NetworkRetries429                prometheus.Counter
	NetworkRetries5XX                prometheus.Counter
	NetworkSentDuration              prometheus.Histogram
	NetworkErrors                    prometheus.Counter
	NetworkNewestOutTimeStampSeconds prometheus.Gauge

	// Filequeue Stats
	FilequeueInSeries                 prometheus.Counter
	FilequeueNewestInTimeStampSeconds prometheus.Gauge
	FilequeueErrors                   prometheus.Counter

	// Backwards compatibility metrics
	RemoteStorageInTimestamp  prometheus.Gauge
	RemoteStorageOutTimestamp prometheus.Gauge
	RemoteStorageDuration     prometheus.Histogram
}

func NewStats(namespace, subsystem string, registry prometheus.Registerer) *PrometheusStats {
	s := &PrometheusStats{
		FilequeueInSeries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "filequeue_incoming_series",
		}),
		RemoteStorageInTimestamp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_remote_storage_highest_timestamp_in_seconds",
		}),
		FilequeueNewestInTimeStampSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "filequeue_incoming_timestamp_seconds",
		}),
		FilequeueErrors: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "filequeue_errors",
		}),
		NetworkNewestOutTimeStampSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "network_timestamp_seconds",
		}),
		RemoteStorageOutTimestamp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_remote_storage_queue_highest_sent_timestamp_seconds",
		}),
		RemoteStorageDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "prometheus_remote_storage_queue_duration_seconds",
		}),
		NetworkSeriesSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "network_series_sent",
		}),
		NetworkFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "network_failures",
		}),
		NetworkRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "network_retries",
		}),
		NetworkRetries429: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "network_retries_429",
		}),
		NetworkRetries5XX: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "network_retries_5xx",
		}),
		NetworkSentDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:                   namespace,
			Subsystem:                   subsystem,
			Name:                        "network_duration_seconds",
			NativeHistogramBucketFactor: 1.1,
		}),
		NetworkErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "network_errors",
		}),
	}
	registry.MustRegister(
		s.NetworkSentDuration,
		s.NetworkRetries5XX,
		s.NetworkRetries429,
		s.NetworkRetries,
		s.NetworkFailures,
		s.NetworkSeriesSent,
		s.NetworkErrors,
		s.NetworkNewestOutTimeStampSeconds,
		s.FilequeueInSeries,
		s.FilequeueErrors,
		s.FilequeueNewestInTimeStampSeconds,
	)
	return s
}

func (s *PrometheusStats) BackwardsCompatibility(registry prometheus.Registerer) {
	registry.MustRegister(
		s.RemoteStorageDuration,
		s.RemoteStorageInTimestamp,
		s.RemoteStorageOutTimestamp,
	)
}

func (s *PrometheusStats) UpdateNetwork(stats NetworkStats) {
	s.NetworkSeriesSent.Add(float64(stats.SeriesSent))
	s.NetworkRetries.Add(float64(stats.Retries))
	s.NetworkFailures.Add(float64(stats.Fails))
	s.NetworkRetries429.Add(float64(stats.Retries429))
	s.NetworkRetries5XX.Add(float64(stats.Retries5XX))
	s.NetworkSentDuration.Observe(stats.SendDuration.Seconds())
	s.RemoteStorageDuration.Observe(stats.SendDuration.Seconds())
	s.RemoteStorageOutTimestamp.Set(float64(stats.NewestTimestamp))
}

func (s *PrometheusStats) UpdateFileQueue(stats FileQueueStats) {
	s.FilequeueInSeries.Add(float64(stats.SeriesStored))
	s.FilequeueErrors.Add(float64(stats.Errors))
	s.FilequeueNewestInTimeStampSeconds.Set(float64(stats.NewestTimestamp))
}

type NetworkStats struct {
	Retries         int
	Retries429      int
	Retries5XX      int
	SendDuration    time.Duration
	SeriesSent      int
	Fails           int
	NewestTimestamp int64
}

type FileQueueStats struct {
	SeriesStored    int
	Errors          int
	NewestTimestamp int64
}
