package network

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
)

func BenchmarkLoopAllocs(b *testing.B) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	b.ReportAllocs()

	logger := log.NewNopLogger()

	l := newLoop(ConnectionConfig{
		URL:                     "http://localhost",
		Timeout:                 1 * time.Second,
		RetryBackoff:            1 * time.Second,
		MaxRetryBackoffAttempts: 0,
		BatchCount:              1_000,
		FlushDuration:           1 * time.Second,
		Connections:             1,
	}, logger, func(s types.NetworkStats) {
	})
	l.clientDo = func(r *http.Request) (*http.Response, error) {
		// Dont actually do anything
		return &http.Response{
			StatusCode: 200,
		}, nil
	}
	b.ResetTimer()
	l.Start()
	defer l.Stop()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		series := getTimeSeries(b)
		b.StartTimer()
		for _, tss := range series {
			l.seriesMbx.Send(context.Background(), tss)
		}
	}
	// Since we arent sure when it will get done then simple wait long enough.
	time.Sleep(5 * time.Second)
	l.Stop()
}

func getTimeSeries(b *testing.B) []*types.TimeSeries {
	b.Helper()
	series := make([]*types.TimeSeries, 0)
	for j := 0; j < 1_000; j++ {
		timeseries := types.GetTimeSeries()
		timeseries.TS = time.Now().Unix()
		timeseries.Value = rand.Float64()
		timeseries.Labels = getLabels()
		series = append(series, timeseries)
	}
	return series
}

func getLabels() []types.Label {
	retLbls := make([]types.Label, 0)
	for i := 0; i < rand.Intn(20); i++ {
		l := types.Label{
			Name:  fmt.Sprintf("label_%d", i),
			Value: fmt.Sprintf("value_%d", i),
		}
		retLbls = append(retLbls, l)
	}
	return retLbls
}
