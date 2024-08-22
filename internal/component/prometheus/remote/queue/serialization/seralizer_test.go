package serialization

import (
	"context"
	"fmt"
	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/grafana/alloy/internal/component/prometheus/remote/queue/types"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRoundTripSerialization(t *testing.T) {
	f := &fqq{}
	l := log.NewNopLogger()
	s, err := NewSerializer(10, 5*time.Second, f, l)
	s.Start()
	defer s.Stop()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		tss := types.GetTimeSeriesBinary()
		for j := 0; j < 10; j++ {
			tss.Labels[j] = labels.Label{
				Name:  fmt.Sprintf("name_%d_%d", i, j),
				Value: fmt.Sprintf("value_%d_%d", i, j),
			}
		}
		s.SendSeries(context.Background(), tss)
	}
	require.Eventually(t, func() bool {
		return len(f.buf) > 0
	}, 5*time.Second, 100*time.Millisecond)
	sg := &types.SeriesGroup{}
	sg, _, err = types.DeserializeToSeriesGroup(sg, f.buf)
	require.NoError(t, err)
	require.Len(t, sg.Series, 10)

}

var _ types.FileStorage = (*fq)(nil)

type fqq struct {
	buf []byte
}

func (f *fqq) Start() {

}

func (f *fqq) Stop() {

}

func (f *fqq) Send(ctx context.Context, meta map[string]string, value []byte) error {
	f.buf, _ = snappy.Decode(nil, value)
	return nil
}
