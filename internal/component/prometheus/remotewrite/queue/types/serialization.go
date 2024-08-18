//go:generate msgp
package types

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

type SeriesGroup struct {
	Strings  []string
	Series   []*TimeSeriesBinary
	Metadata []*MetaSeriesBinary
}

type TimeSeriesBinary struct {
	// Labels are not serialized but are passed in.
	Labels       labels.Labels `msg:"-"`
	LabelsNames  []int32
	LabelsValues []int32
	TS           int64
	Value        float64
	Hash         uint64
	Histograms   Histograms
}

type MetaSeriesBinary struct {
	LabelsNames  []int32
	LabelsValues []int32
	TS           int64
	Value        float64
	Hash         uint64
	Histograms   Histograms
}

type Histograms struct {
	Histogram      *Histogram
	FloatHistogram *FloatHistogram
}

type Histogram struct {
	Count                HistogramCount
	Sum                  float64
	Schema               int32
	ZeroThreshold        float64
	ZeroCount            HistogramZeroCount
	NegativeSpans        []BucketSpan
	NegativeBuckets      []int64
	NegativeCounts       []float64
	PositiveSpans        []BucketSpan
	PositiveBuckets      []int64
	PositiveCounts       []float64
	ResetHint            int32
	TimestampMillisecond int64
}

type FloatHistogram struct {
	Count                HistogramCount
	Sum                  float64
	Schema               int32
	ZeroThreshold        float64
	ZeroCount            HistogramZeroCount
	NegativeSpans        []BucketSpan
	NegativeDeltas       []int64
	NegativeCounts       []float64
	PositiveSpans        []BucketSpan
	PositiveDeltas       []int64
	PositiveCounts       []float64
	ResetHint            int32
	TimestampMillisecond int64
}

type HistogramCount struct {
	IsInt      bool
	IntValue   uint64
	FloatValue float64
}

type HistogramZeroCount struct {
	IsInt      bool
	IntValue   uint64
	FloatValue float64
}

type BucketSpan struct {
	Offset int32
	Length uint32
}

func (h *Histogram) ToPromHistogram() prompb.Histogram {
	return prompb.Histogram{
		Count:          &prompb.Histogram_CountInt{CountInt: h.Count.IntValue},
		Sum:            h.Sum,
		Schema:         h.Schema,
		ZeroThreshold:  h.ZeroThreshold,
		ZeroCount:      &prompb.Histogram_ZeroCountInt{ZeroCountInt: h.ZeroCount.IntValue},
		NegativeSpans:  ToPromBucketSpans(h.NegativeSpans),
		NegativeDeltas: h.NegativeBuckets,
		NegativeCounts: h.NegativeCounts,
		PositiveSpans:  ToPromBucketSpans(h.PositiveSpans),
		PositiveDeltas: h.PositiveBuckets,
		PositiveCounts: h.NegativeCounts,
		ResetHint:      prompb.Histogram_ResetHint(h.ResetHint),
		Timestamp:      h.TimestampMillisecond,
	}
}

func (h *FloatHistogram) ToPromFloatHistogram() prompb.Histogram {
	return prompb.Histogram{
		Count:          &prompb.Histogram_CountFloat{CountFloat: h.Count.FloatValue},
		Sum:            h.Sum,
		Schema:         h.Schema,
		ZeroThreshold:  h.ZeroThreshold,
		ZeroCount:      &prompb.Histogram_ZeroCountFloat{ZeroCountFloat: h.ZeroCount.FloatValue},
		NegativeSpans:  ToPromBucketSpans(h.NegativeSpans),
		NegativeDeltas: h.NegativeDeltas,
		NegativeCounts: h.NegativeCounts,
		PositiveSpans:  ToPromBucketSpans(h.PositiveSpans),
		PositiveDeltas: h.PositiveDeltas,
		PositiveCounts: h.PositiveCounts,
		ResetHint:      prompb.Histogram_ResetHint(h.ResetHint),
		Timestamp:      h.TimestampMillisecond,
	}
}
func ToPromBucketSpans(bss []BucketSpan) []prompb.BucketSpan {
	spans := make([]prompb.BucketSpan, len(bss))
	for i, bs := range bss {
		spans[i] = bs.ToPromBucketSpan()
	}
	return spans
}

func (bs *BucketSpan) ToPromBucketSpan() prompb.BucketSpan {
	return prompb.BucketSpan{
		Offset: bs.Offset,
		Length: bs.Length,
	}
}

func (ts *TimeSeriesBinary) FromHistogram(timestamp int64, h *histogram.Histogram) {
	ts.Histograms.Histogram = &Histogram{
		Count:                HistogramCount{IsInt: true, IntValue: h.Count},
		Sum:                  h.Sum,
		Schema:               h.Schema,
		ZeroThreshold:        h.ZeroThreshold,
		ZeroCount:            HistogramZeroCount{IsInt: true, IntValue: h.ZeroCount},
		NegativeSpans:        FromPromSpan(h.NegativeSpans),
		NegativeBuckets:      h.NegativeBuckets,
		PositiveSpans:        FromPromSpan(h.PositiveSpans),
		PositiveBuckets:      h.PositiveBuckets,
		ResetHint:            int32(h.CounterResetHint),
		TimestampMillisecond: timestamp,
	}
}
func (ts *TimeSeriesBinary) FromFloatHistogram(timestamp int64, h *histogram.FloatHistogram) {
	ts.Histograms.FloatHistogram = &FloatHistogram{
		Count:                HistogramCount{IsInt: false, FloatValue: h.Count},
		Sum:                  h.Sum,
		Schema:               h.Schema,
		ZeroThreshold:        h.ZeroThreshold,
		ZeroCount:            HistogramZeroCount{IsInt: false, FloatValue: h.ZeroCount},
		NegativeSpans:        FromPromSpan(h.NegativeSpans),
		NegativeCounts:       h.NegativeBuckets,
		PositiveSpans:        FromPromSpan(h.PositiveSpans),
		PositiveCounts:       h.PositiveBuckets,
		ResetHint:            int32(h.CounterResetHint),
		TimestampMillisecond: timestamp,
	}
}
func FromPromSpan(spans []histogram.Span) []BucketSpan {
	bs := make([]BucketSpan, len(spans))
	for i, s := range spans {
		bs[i].Offset = s.Offset
		bs[i].Length = s.Length
	}
	return bs
}
