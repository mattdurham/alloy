package types

import (
	"context"
	"go.uber.org/atomic"
	"sync"
)

type FileStorage interface {
	Start()
	Stop()
	Send(ctx context.Context, meta map[string]string, value []byte) error
}

type Serializer interface {
	Start()
	Stop()
	SendSeries(ctx context.Context, data []*TimeSeries) error
	SendMetadata(ctx context.Context, data []*MetaSeries) error
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

func GetTimeSeriesBinarySlice(n int) []*TimeSeriesBinary {
	tss := make([]*TimeSeriesBinary, 0, n)
	for i := 0; i < n; i++ {
		tss = append(tss, GetTimeSeriesBinary())
	}
	return tss
}

func PutTimeSeriesBinarySlice(tss []*TimeSeriesBinary) {
	for _, ts := range tss {
		PutTimeSeriesBinary(ts)
	}
}

func PutTimeSeriesBinarySliceValue(tss []TimeSeriesBinary) {
	for _, ts := range tss {
		PutTimeSeriesBinary(&ts)
	}
}

func PutTimeSeriesBinary(ts *TimeSeriesBinary) {
	OutStandingTimeSeriesBinary.Dec()
	ts.LabelsNames = ts.LabelsNames[:0]
	ts.LabelsValues = ts.LabelsValues[:0]
	ts.TS = 0
	ts.Value = 0
	ts.Hash = 0
	ts.Histograms.Histogram = nil
	ts.Histograms.FloatHistogram = nil
	tsBinaryPool.Put(ts)
}

func BinaryToTimeSeries(bin *TimeSeriesBinary, items []string) *TimeSeries {
	ts := GetTimeSeries()
	ts.TS = bin.TS
	ts.Hash = bin.Hash
	ts.Histogram = bin.Histograms.Histogram
	ts.FloatHistogram = bin.Histograms.FloatHistogram
	ts.Value = bin.Value
	if cap(ts.Labels) < len(bin.LabelsValues) {
		ts.Labels = make([]Label, len(bin.LabelsValues))
	} else {
		ts.Labels = ts.Labels[:len(bin.LabelsValues)]
	}

	for i := 0; i < len(bin.LabelsNames); i++ {
		ts.Labels[i].Name = items[bin.LabelsNames[i]]
		ts.Labels[i].Value = items[bin.LabelsValues[i]]
	}
	return ts
}

func DeserializeToSeriesGroup(sg *SeriesGroup, buf []byte) (*SeriesGroup, error) {
	buf, err := sg.UnmarshalMsg(buf)
	return sg, err
}
