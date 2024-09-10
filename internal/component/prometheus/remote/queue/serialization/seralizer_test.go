package serialization

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/grafana/alloy/internal/component/prometheus/remote/queue/types"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestRoundTripSerialization(t *testing.T) {
	f := &fqq{t: t}
	l := log.NewNopLogger()
	s, err := NewSerializer(types.SerializerConfig{
		MaxSignalsInBatch: 10,
		FlushFrequency:    5 * time.Second,
	}, f, l)
	s.Start()
	defer s.Stop()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		tss := types.GetTimeSeriesBinary()
		tss.Labels = make(labels.Labels, 10)
		for j := 0; j < 10; j++ {
			tss.Labels[j] = labels.Label{
				Name:  fmt.Sprintf("name_%d_%d", i, j),
				Value: fmt.Sprintf("value_%d_%d", i, j),
			}
			tss.Value = float64(i)
		}
		sendErr := s.SendSeries(context.Background(), tss)
		require.NoError(t, sendErr)
	}
	require.Eventually(t, func() bool {
		return f.total == 100
	}, 5*time.Second, 100*time.Millisecond)
}

var _ types.FileStorage = (*fq)(nil)

type fqq struct {
	t     *testing.T
	buf   []byte
	total int
}

func (f *fqq) Start() {

}

func (f *fqq) Stop() {

}

func (f *fqq) Send(ctx context.Context, meta map[string]string, value []byte) error {
	f.buf, _ = snappy.Decode(nil, value)
	sg := &types.SeriesGroup{}
	sg, _, err := types.DeserializeToSeriesGroup(sg, f.buf)
	require.NoError(f.t, err)
	require.Len(f.t, sg.Series, 10)
	for _, series := range sg.Series {
		require.Len(f.t, series.LabelsNames, 0)
		require.Len(f.t, series.LabelsValues, 0)
		require.Len(f.t, series.Labels, 10)
		for j := 0; j < 10; j++ {
			series.Labels[j].Name = fmt.Sprintf("name_%d_%d", int(series.Value), j)
			series.Labels[j].Value = fmt.Sprintf("value_%d_%d", int(series.Value), j)
		}
	}
	f.total += len(sg.Series)
	return nil
}
