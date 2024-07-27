package queue

import (
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/network"
	"github.com/prometheus/client_golang/prometheus"
)

type Stats struct {
	SeriesSent   prometheus.Counter
	Failures     prometheus.Counter
	Retries      prometheus.Counter
	Retries429   prometheus.Counter
	Retry5XX     prometheus.Counter
	SentDuration prometheus.Histogram
	Errors       prometheus.Counter
}

func NewStats(namespace, subsystem string, registry prometheus.Registerer) *Stats {
	s := &Stats{
		SeriesSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "series_sent",
		}),
		Failures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "failures",
		}),
		Retries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "retries",
		}),
		Retries429: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "retries_429",
		}),
		Retries5XX: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "retries_5xx",
		}),
		SentDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:                   namespace,
			Subsystem:                   subsystem,
			Name:                        "duration",
			NativeHistogramBucketFactor: 1.1,
		}),
		Errors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "errors",
		}),
	}
	return s
}

func (s *Stats) update(stats network.Stats) {
	s.SeriesSent.Add(float64(stats.SeriesSent))
	s.Retries.Add(float64(stats.Retries))
	s.Failures.Add(float64(stats.Fails))
	s.Retries429.Add(float64(stats.Retries429))
	s.Retry5XX.Add(float64(stats.Retries5XX))
	s.SentDuration.Observe(float64(stats.SendDurationSeconds))
}
