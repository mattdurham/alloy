package types

import (
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

type PrometheusStats struct {
	SeriesSent   prometheus.Counter
	Failures     prometheus.Counter
	Retries      prometheus.Counter
	Retries429   prometheus.Counter
	Retries5XX   prometheus.Counter
	SentDuration prometheus.Histogram
	Errors       prometheus.Counter
}

func NewStats(namespace, subsystem string, registry prometheus.Registerer) *PrometheusStats {
	s := &PrometheusStats{
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
	registry.MustRegister(s.SentDuration, s.Retries5XX, s.Retries429, s.Retries, s.Failures, s.SeriesSent, s.Errors)
	return s
}

func (s *PrometheusStats) Update(stats Stats) {
	s.SeriesSent.Add(float64(stats.SeriesSent))
	s.Retries.Add(float64(stats.Retries))
	s.Failures.Add(float64(stats.Fails))
	s.Retries429.Add(float64(stats.Retries429))
	s.Retries5XX.Add(float64(stats.Retries5XX))
	s.SentDuration.Observe(float64(stats.SendDuration))
}

type Stats struct {
	SeriesSent   int
	Fails        int
	Retries      int
	Retries429   int
	Retries5XX   int
	SendDuration time.Duration
}
