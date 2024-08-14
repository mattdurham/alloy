package queue

import (
	"context"
	"fmt"
	"github.com/golang/snappy"
	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/grafana/alloy/internal/runtime/logging"
	"github.com/grafana/alloy/internal/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestE2ESamples(t *testing.T) {
	l := util.TestAlloyLogger(t)
	done := make(chan struct{})
	var series atomic.Int32
	samples := make([]prompb.TimeSeries, 0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newSamples := handlePost(t, w, r)
		series.Add(int32(len(newSamples)))
		samples = append(samples, newSamples...)
		l.Log("msg", "receiving data", "total", series.Load())

		if series.Load() == 1_000 {
			done <- struct{}{}
		}
	}))
	expCh := make(chan types.Exports, 1)
	c, err := newComponent(t, l, srv.URL, expCh)
	require.NoError(t, err)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		runErr := c.Run(ctx)
		require.NoError(t, runErr)
	}()
	exp := <-expCh
	index := 0
	for i := 0; i < 10; i++ {
		app := exp.Receiver.Appender(ctx)
		for j := 0; j < 100; j++ {
			index++
			ts, v, lbls := makeSeries(index)
			_, errApp := app.Append(0, lbls, ts, v)
			require.NoError(t, errApp)
		}
		require.NoError(t, app.Commit())
	}
	tm := time.NewTimer(15 * time.Second)
	select {
	case <-done:
	case <-tm.C:
	}
	cancel()
	for _, s := range samples {
		require.True(t, len(s.Samples) == 1)
		require.True(t, s.Samples[0].Timestamp > 0)
		require.True(t, s.Samples[0].Value > 0)
		require.True(t, len(s.Labels) == 1)
		require.True(t, s.Labels[0].Name == fmt.Sprintf("name_%d", int(s.Samples[0].Value)))
		require.True(t, s.Labels[0].Value == fmt.Sprintf("value_%d", int(s.Samples[0].Value)))
	}
}

func TestE2EHistogram(t *testing.T) {
	l := util.TestAlloyLogger(t)
	done := make(chan struct{})
	var series atomic.Int32
	samples := make([]prompb.TimeSeries, 0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newSamples := handlePost(t, w, r)
		series.Add(int32(len(newSamples)))
		samples = append(samples, newSamples...)
		if series.Load() == 1_000 {
			done <- struct{}{}
		}
	}))
	expCh := make(chan types.Exports, 1)
	c, err := newComponent(t, l, srv.URL, expCh)
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
	for i := 0; i < 10; i++ {
		app := exp.Receiver.Appender(ctx)
		for j := 0; j < 100; j++ {
			index++
			ts, lbls, h := makeHistogram(index)
			_, errApp := app.AppendHistogram(0, lbls, ts, h, nil)
			require.NoError(t, errApp)
		}
		require.NoError(t, app.Commit())
	}
	l.Log("msg", "waiting")
	tm := time.NewTimer(15 * time.Second)
	select {
	case <-done:
	case <-tm.C:
	}
	cancel()
	for _, s := range samples {
		require.True(t, len(s.Samples) == 1)
		require.True(t, s.Samples[0].Timestamp > 0)
		require.True(t, s.Samples[0].Value == 0)
		require.True(t, len(s.Labels) == 1)
		histSame(t, hist(), s.Histograms[0])
	}
}

func handlePost(t *testing.T, w http.ResponseWriter, r *http.Request) []prompb.TimeSeries {
	defer r.Body.Close()
	data, err := io.ReadAll(r.Body)
	require.NoError(t, err)

	data, err = snappy.Decode(nil, data)
	require.NoError(t, err)

	var req prompb.WriteRequest
	err = req.Unmarshal(data)
	require.NoError(t, err)
	return req.GetTimeseries()

}

func makeSeries(index int) (int64, float64, labels.Labels) {
	return time.Now().UTC().Unix(), float64(index), labels.FromStrings(fmt.Sprintf("name_%d", index), fmt.Sprintf("value_%d", index))
}

func makeHistogram(index int) (int64, labels.Labels, *histogram.Histogram) {
	return time.Now().UTC().Unix(), labels.FromStrings(fmt.Sprintf("name_%d", index), fmt.Sprintf("value_%d", index)), hist()
}

func hist() *histogram.Histogram {
	return &histogram.Histogram{
		CounterResetHint: 1,
		Schema:           2,
		ZeroThreshold:    3,
		ZeroCount:        4,
		Count:            5,
		Sum:              6,
		PositiveSpans: []histogram.Span{
			{
				Offset: 1,
				Length: 2,
			},
		},
		NegativeSpans: []histogram.Span{
			{
				Offset: 3,
				Length: 4,
			},
		},
		PositiveBuckets: []int64{1, 2, 3},
		NegativeBuckets: []int64{1, 2, 3},
	}
}

func histSame(t *testing.T, h *histogram.Histogram, pb prompb.Histogram) {
	require.True(t, h.Sum == pb.Sum)
	require.True(t, h.ZeroCount == pb.ZeroCount.(*prompb.Histogram_ZeroCountInt).ZeroCountInt)
	require.True(t, h.Schema == pb.Schema)
	require.True(t, h.Count == pb.Count.(*prompb.Histogram_CountInt).CountInt)
	require.True(t, h.ZeroThreshold == pb.ZeroThreshold)
	require.True(t, int32(h.CounterResetHint) == int32(pb.ResetHint))
	require.True(t, reflect.DeepEqual(h.PositiveBuckets, pb.PositiveDeltas))
	require.True(t, reflect.DeepEqual(h.NegativeBuckets, pb.NegativeDeltas))
	histSpanSame(t, h.PositiveSpans, pb.PositiveSpans)
	histSpanSame(t, h.NegativeSpans, pb.NegativeSpans)

}

func histSpanSame(t *testing.T, h []histogram.Span, pb []prompb.BucketSpan) {
	require.True(t, len(h) == len(pb))
	for i := range h {
		require.True(t, h[i].Length == pb[i].Length)
		require.True(t, h[i].Offset == pb[i].Offset)
	}
}

func newComponent(t *testing.T, l *logging.Logger, url string, exp chan types.Exports) (*Queue, error) {
	return NewComponent(component.Options{
		ID:       "test",
		Logger:   l,
		DataPath: t.TempDir(),
		OnStateChange: func(e component.Exports) {
			exp <- e.(types.Exports)
		},
		Registerer: prometheus.DefaultRegisterer,
		Tracer:     nil,
	}, types.Arguments{
		TTL:            2 * time.Hour,
		BatchSizeBytes: 16 * 1024 * 1024,
		FlushDuration:  1 * time.Second,
		Connections: []types.ConnectionConfig{{
			Name:                    "test",
			URL:                     url,
			Timeout:                 10 * time.Second,
			RetryBackoff:            1 * time.Second,
			MaxRetryBackoffAttempts: 0,
			BatchCount:              50,
			FlushDuration:           1 * time.Second,
			QueueCount:              1,
		}},
		AppenderBatchSize: 1_000,
		ExternalLabels:    nil,
	})
}
