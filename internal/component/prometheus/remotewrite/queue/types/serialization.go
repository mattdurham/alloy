//go:generate msgp
package types

import "github.com/prometheus/prometheus/model/labels"

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
