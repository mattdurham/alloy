package types

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"
)

type FileStorage interface {
	Start()
	Stop()
	Send(ctx context.Context, meta map[string]string, value []byte) error
}

type Serializer interface {
	Start()
	Stop()
	SendSeries(ctx context.Context, data *TimeSeriesBinary) error
	SendMetadata(ctx context.Context, data *MetaSeriesBinary) error
}

var tsBinaryPool = sync.Pool{
	New: func() any {
		return &TimeSeriesBinary{}
	},
}

func GetTimeSeriesBinary() *TimeSeriesBinary {
	OutStandingTimeSeriesBinary.Inc()
	return tsBinaryPool.Get().(*TimeSeriesBinary)
}

var OutStandingTimeSeriesBinary = atomic.Int32{}

func PutTimeSeriesBinarySlice(tss []*TimeSeriesBinary) {
	for i := 0; i < len(tss); i++ {
		PutTimeSeriesBinary(tss[i])
	}

}

func PutTimeSeriesBinary(ts *TimeSeriesBinary) {
	OutStandingTimeSeriesBinary.Dec()
	ts.LabelsNames = ts.LabelsNames[:0]
	ts.LabelsValues = ts.LabelsValues[:0]
	ts.Labels = nil
	ts.TS = 0
	ts.Value = 0
	ts.Hash = 0
	ts.Histograms.Histogram = nil
	ts.Histograms.FloatHistogram = nil
	tsBinaryPool.Put(ts)
}

func DeserializeToSeriesGroup(sg *SeriesGroup, buf []byte) (*SeriesGroup, []byte, error) {
	buffer, err := sg.UnmarshalMsg(buf)
	// Need to fill in the labels.
	for _, series := range sg.Series {
		if cap(series.Labels) < len(series.LabelsNames) {
			series.Labels = make(labels.Labels, len(series.LabelsNames))
		} else {
			series.Labels = series.Labels[:len(series.LabelsNames)]
		}
		for i := range series.LabelsNames {
			series.Labels[i] = labels.Label{
				Name:  sg.Strings[series.LabelsNames[i]],
				Value: sg.Strings[series.LabelsValues[i]],
			}
		}
	}
	sg.Strings = sg.Strings[:0]
	return sg, buffer, err
}
