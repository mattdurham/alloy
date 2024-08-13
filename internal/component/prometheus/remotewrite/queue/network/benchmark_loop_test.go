package network

import (
	"context"
	"fmt"
	"github.com/go-kit/log"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/prometheus/prometheus/prompb"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func BenchmarkLoopAllocs(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		return
	}))
	logger := log.NewNopLogger()
	s := prompb.Sample{
		Value:     0,
		Timestamp: time.Now().Unix(),
	}
	lbls := make([]prompb.Label, 0)
	for i := 0; i < 20; i++ {
		lbls = append(lbls, prompb.Label{
			Name:  fmt.Sprintf("label_%d", i),
			Value: fmt.Sprintf("value_%d", i),
		})
	}
	ts := prompb.TimeSeries{
		Samples: []prompb.Sample{s},
		Labels:  lbls,
	}
	buf, _ := ts.Marshal()
	l := newLoop(ConnectionConfig{
		URL:                     srv.URL,
		Timeout:                 1 * time.Second,
		RetryBackoff:            1 * time.Second,
		MaxRetryBackoffAttempts: 0,
		BatchCount:              1_000,
		FlushDuration:           1 * time.Second,
		Connections:             1,
	}, logger, func(s types.NetworkStats) {

	})
	l.Start()
	defer l.Stop()
	for i := 0; i < 10_000; i++ {
		l.seriesMbx.Send(context.Background(), buf)
	}
	l.Stop()

}
