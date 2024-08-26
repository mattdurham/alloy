package queue

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/alloy/internal/component/prometheus/remote/queue/types"
	"github.com/grafana/alloy/internal/util"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

const remoteSamples = "prometheus_remote_storage_samples_total"
const remoteExemplars = "prometheus_remote_storage_exemplars_total"
const remoteHistograms = "prometheus_remote_storage_histograms_total"
const remoteMetadata = "prometheus_remote_storage_metadata_total"

const sentBytes = "prometheus_remote_storage_sent_bytes_total"
const sentMetadataBytes = "prometheus_remote_storage_metadata_bytes_total"

const outTimestamp = "prometheus_remote_storage_queue_highest_sent_timestamp_seconds"
const inTimestamp = "prometheus_remote_storage_highest_timestamp_in_seconds"

const failedSample = "prometheus_remote_storage_samples_failed_total"
const failedHistogram = "prometheus_remote_storage_histograms_failed_total"
const failedMetadata = "prometheus_remote_storage_metadata_failed_total"

const retriedSamples = "prometheus_remote_storage_samples_retried_total"
const retriedExemplars = "prometheus_remote_storage_exemplars_retried_total"
const retriedHistogram = "prometheus_remote_storage_histograms_retried_total"
const retriedMetadata = "prometheus_remote_storage_metadata_retried_total"

func TestMetrics(t *testing.T) {
	tests := []statsTest{
		{
			name:             "basic success",
			returnStatusCode: http.StatusOK,
			check: func(metrics map[string]float64) {
				checkValue(t, remoteSamples, 10, metrics)
				checkValueCondition(t, sentBytes, func(v float64) bool { return v > 0 }, metrics)
				checkValue(t, sentMetadataBytes, 0, metrics)
				checkValueCondition(t, outTimestamp, func(v float64) bool {
					require.True(t, v > 0)
					unixTime := time.Unix(int64(v), 0)
					require.True(t, v > 0)
					require.True(t, time.Since(unixTime) < 10*time.Second)
					return true
				}, metrics)
				checkValueCondition(t, inTimestamp, func(v float64) bool {
					require.True(t, v > 0)
					unixTime := time.Unix(int64(v), 0)
					require.True(t, v > 0)
					require.True(t, time.Since(unixTime) < 10*time.Second)
					return true
				}, metrics)
			},
		},
		{
			name:             "basic failure",
			returnStatusCode: http.StatusBadRequest,
			check: func(metrics map[string]float64) {
				checkValue(t, failedSample, 10, metrics)
				checkValue(t, remoteSamples, 0, metrics)
				checkValue(t, sentBytes, 0, metrics)
				checkValue(t, sentMetadataBytes, 0, metrics)
				checkValue(t, outTimestamp, 0, metrics)
				// In is the incoming to the queue so should not be affected by the failure or success of
				// the network.
				checkValueCondition(t, inTimestamp, func(v float64) bool {
					require.True(t, v > 0)
					unixTime := time.Unix(int64(v), 0)
					require.True(t, v > 0)
					require.True(t, time.Since(unixTime) < 10*time.Second)
					return true
				}, metrics)
			},
		},
		{
			name:             "basic retry  ",
			returnStatusCode: http.StatusTooManyRequests,
			check: func(metrics map[string]float64) {
				checkValue(t, retriedSamples, 10, metrics)
				checkValue(t, failedSample, 0, metrics)
				checkValue(t, remoteSamples, 0, metrics)
				checkValue(t, sentBytes, 0, metrics)
				checkValue(t, sentMetadataBytes, 0, metrics)
				checkValue(t, outTimestamp, 0, metrics)
				// In is the incoming to the queue so should not be affected by the failure or success of
				// the network.
				checkValueCondition(t, inTimestamp, func(v float64) bool {
					require.True(t, v > 0)
					unixTime := time.Unix(int64(v), 0)
					require.True(t, v > 0)
					require.True(t, time.Since(unixTime) < 10*time.Second)
					return true
				}, metrics)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runE2eStats(t, test)
		})
	}

}

type statsTest struct {
	name             string
	returnStatusCode int
	check            func(metrics map[string]float64)
}

func runE2eStats(t *testing.T, test statsTest) {
	l := util.TestAlloyLogger(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(test.returnStatusCode)
	}))
	expCh := make(chan types.Exports, 1)

	reg := prometheus.NewRegistry()
	c, err := newComponent(t, l, srv.URL, expCh, reg)
	require.NoError(t, err)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		runErr := c.Run(ctx)
		require.NoError(t, runErr)
	}()
	// Wait for export to spin up.
	exp := <-expCh

	index := 0

	go func() {
		app := exp.Receiver.Appender(ctx)
		for j := 0; j < 10; j++ {
			index++
			ts, v, lbls := makeSeries(index)
			_, errApp := app.Append(0, lbls, ts, v)
			require.NoError(t, errApp)
		}
		require.NoError(t, app.Commit())
	}()
	tm := time.NewTimer(8 * time.Second)
	<-tm.C
	cancel()

	require.Eventually(t, func() bool {
		dtos, gatherErr := reg.Gather()
		require.NoError(t, gatherErr)
		for _, d := range dtos {
			if getValue(d) > 0 {
				return true
			}
		}
		return false
	}, 10*time.Second, 1*time.Second)
	metrics := make(map[string]float64)
	dtos, err := reg.Gather()
	require.NoError(t, err)
	for _, d := range dtos {
		metrics[*d.Name] = getValue(d)
	}

	test.check(metrics)
}

func getValue(d *dto.MetricFamily) float64 {
	switch *d.Type {
	case dto.MetricType_COUNTER:
		return d.Metric[0].Counter.GetValue()
	case dto.MetricType_GAUGE:
		return d.Metric[0].Gauge.GetValue()
	case dto.MetricType_SUMMARY:
		return d.Metric[0].Summary.GetSampleSum()
	case dto.MetricType_UNTYPED:
		return d.Metric[0].Untyped.GetValue()
	case dto.MetricType_HISTOGRAM:
		return d.Metric[0].Histogram.GetSampleSum()
	case dto.MetricType_GAUGE_HISTOGRAM:
		return d.Metric[0].Histogram.GetSampleSum()
	default:
		panic("unknown type " + d.Type.String())
	}
}

func checkValue(t *testing.T, name string, value float64, metrics map[string]float64) {
	v, ok := metrics[name]
	require.Truef(t, ok, "invalid metric name %s", name)
	require.Equal(t, value, v)
}

func checkValueCondition(t *testing.T, name string, chk func(float64) bool, metrics map[string]float64) {
	v, ok := metrics[name]
	require.True(t, ok)
	require.True(t, chk(v))
}
