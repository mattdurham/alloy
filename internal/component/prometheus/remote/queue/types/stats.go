package types

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
	SamplesTotal              prometheus.Counter
	ExemplarsTotal            prometheus.Counter
	HistogramsTotal           prometheus.Counter
	MetadataTotal             prometheus.Counter
	FailedSamplesTotal        prometheus.Counter
	FailedHistogramsTotal     prometheus.Counter
	FailedMetadataTotal       prometheus.Counter
	FailedExemplarsTotal      prometheus.Counter
	RetriedSamplesTotal       prometheus.Counter
	RetriedExemplarsTotal     prometheus.Counter
	RetriedHistogramsTotal    prometheus.Counter
	RetriedMetadataTotal      prometheus.Counter
	EnqueueRetriesTotal       prometheus.Counter
	SentBatchDuration         prometheus.Histogram
	HighestSentTimestamp      prometheus.Gauge
	PendingSamples            prometheus.Gauge
	PendingExemplars          prometheus.Gauge
	PendingHistograms         prometheus.Gauge
	SentBytesTotal            prometheus.Counter
	MetadataBytesTotal        prometheus.Counter
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
		SamplesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_samples_total",
			Help: "Total number of samples sent to remote storage.",
		}),
		ExemplarsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_exemplars_total",
			Help: "Total number of exemplars sent to remote storage.",
		}),
		HistogramsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_histograms_total",
			Help: "Total number of exemplars sent to remote storage.",
		}),
		MetadataTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_metadata_total",
			Help: "Total number of metadata sent to remote storage.",
		}),
		FailedSamplesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_samples_failed_total",
			Help: "Total number of samples which failed on send to remote storage, non-recoverable errors.",
		}),
		FailedHistogramsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_histograms_failed_total",
			Help: "Total number of histograms which failed on send to remote storage, non-recoverable errors.",
		}),
		FailedMetadataTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_metadata_failed_total",
			Help: "Total number of metadata entries which failed on send to remote storage, non-recoverable errors.",
		}),
		FailedExemplarsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_exemplar_failed_total",
			Help: "Total number of exemplars which failed on send to remote storage, non-recoverable errors.",
		}),

		RetriedSamplesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_samples_retried_total",
			Help: "Total number of samples which failed on send to remote storage but were retried because the send error was recoverable.",
		}),
		RetriedExemplarsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_exemplars_retried_total",
			Help: "Total number of exemplars which failed on send to remote storage but were retried because the send error was recoverable.",
		}),
		RetriedHistogramsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_histograms_retried_total",
			Help: "Total number of histograms which failed on send to remote storage but were retried because the send error was recoverable.",
		}),
		RetriedMetadataTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_metadata_retried_total",
			Help: "Total number of metadata entries which failed on send to remote storage but were retried because the send error was recoverable.",
		}),
		SentBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_sent_bytes_total",
			Help: "The total number of bytes of data (not metadata) sent by the queue after compression. Note that when exemplars over remote write is enabled the exemplars included in a remote write request count towards this metric.",
		}),
		MetadataBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_metadata_bytes_total",
			Help: "The total number of bytes of metadata sent by the queue after compression.",
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
		s.SamplesTotal,
		s.ExemplarsTotal,
		s.HistogramsTotal,
		s.MetadataTotal,
		s.FailedSamplesTotal,
		s.FailedHistogramsTotal,
		s.FailedMetadataTotal,
		s.FailedExemplarsTotal,
		s.RetriedSamplesTotal,
		s.RetriedExemplarsTotal,
		s.RetriedHistogramsTotal,
		s.RetriedMetadataTotal,
		s.SentBytesTotal,
		s.MetadataBytesTotal,
	)
}

func (s *PrometheusStats) UpdateNetwork(stats NetworkStats) {
	s.NetworkSeriesSent.Add(float64(stats.TotalSent()))
	s.NetworkRetries.Add(float64(stats.TotalRetried()))
	s.NetworkFailures.Add(float64(stats.TotalFailed()))
	s.NetworkRetries429.Add(float64(stats.Total429()))
	s.NetworkRetries5XX.Add(float64(stats.Total5XX()))
	s.NetworkSentDuration.Observe(stats.SendDuration.Seconds())
	s.RemoteStorageDuration.Observe(stats.SendDuration.Seconds())
	// The newest timestamp is no always sent.
	if stats.NewestTimestamp != 0 {
		s.RemoteStorageOutTimestamp.Set(float64(stats.NewestTimestamp))
	}

	s.SamplesTotal.Add(float64(stats.Series.SeriesSent))
	s.ExemplarsTotal.Add(float64(stats.Exemplars.SeriesSent))
	s.MetadataTotal.Add(float64(stats.Metadata.SeriesSent))
	s.HistogramsTotal.Add(float64(stats.Histogram.SeriesSent))

	s.FailedSamplesTotal.Add(float64(stats.Series.Fails))
	s.FailedMetadataTotal.Add(float64(stats.Metadata.Fails))
	s.FailedHistogramsTotal.Add(float64(stats.Histogram.Fails))
	s.FailedExemplarsTotal.Add(float64(stats.Exemplars.Fails))

	s.RetriedSamplesTotal.Add(float64(stats.Series.Retries))
	s.RetriedExemplarsTotal.Add(float64(stats.Exemplars.Retries))
	s.RetriedHistogramsTotal.Add(float64(stats.Histogram.Retries))
	s.RetriedMetadataTotal.Add(float64(stats.Metadata.Retries))

	s.MetadataBytesTotal.Add(float64(stats.MetadataBytes))
	s.SentBytesTotal.Add(float64(stats.SeriesBytes))
}

func (s *PrometheusStats) UpdateFileQueue(stats FileQueueStats) {
	s.FilequeueInSeries.Add(float64(stats.SeriesStored))
	s.FilequeueErrors.Add(float64(stats.Errors))
	if stats.NewestTimestamp != 0 {
		s.FilequeueNewestInTimeStampSeconds.Set(float64(stats.NewestTimestamp))
		s.RemoteStorageInTimestamp.Set(float64(stats.NewestTimestamp))
	}
}

type NetworkStats struct {
	Series          CategoryStats
	Histogram       CategoryStats
	Exemplars       CategoryStats
	Metadata        CategoryStats
	SendDuration    time.Duration
	NewestTimestamp int64
	SeriesBytes     int
	MetadataBytes   int
}

func (ns NetworkStats) TotalSent() int {
	return ns.Series.SeriesSent + ns.Histogram.SeriesSent + ns.Exemplars.SeriesSent + ns.Metadata.SeriesSent
}

func (ns NetworkStats) TotalRetried() int {
	return ns.Series.Retries + ns.Histogram.Retries + ns.Exemplars.Retries + ns.Metadata.Retries
}

func (ns NetworkStats) TotalFailed() int {
	return ns.Series.Fails + ns.Histogram.Fails + ns.Exemplars.Fails + ns.Metadata.Fails
}

func (ns NetworkStats) Total429() int {
	return ns.Series.Retries429 + ns.Histogram.Retries429 + ns.Exemplars.Retries429 + ns.Metadata.Retries429
}

func (ns NetworkStats) Total5XX() int {
	return ns.Series.Retries5XX + ns.Histogram.Retries5XX + ns.Exemplars.Retries5XX + ns.Metadata.Retries5XX
}

type CategoryStats struct {
	Retries    int
	Retries429 int
	Retries5XX int
	SeriesSent int
	Fails      int
}

type FileQueueStats struct {
	SeriesStored    int
	Errors          int
	NewestTimestamp int64
}
