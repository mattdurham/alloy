package types

import (
	"fmt"
	"time"

	"github.com/grafana/alloy/syntax/alloytypes"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
)

func (h Histogram) ToPromHistogram() prompb.Histogram {
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

func (h FloatHistogram) ToPromFloatHistogram() prompb.Histogram {
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

func (bs BucketSpan) ToPromBucketSpan() prompb.BucketSpan {
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

/*
var tsPool = sync.Pool{
	New: func() any {
		return &TimeSeries{}
	},
}

// GetTimeSeries returns a TimeSeries from the pool, when entirely done it should be returned to the pool.
func GetTimeSeries() *TimeSeries {
	OutStandingTimeSeries.Inc()
	return tsPool.Get().(*TimeSeries)
}

var OutStandingTimeSeries = atomic.Int32{}

func GetTimeSeriesSlice(n int) []*TimeSeries {
	tss := make([]*TimeSeries, 0, n)
	for i := 0; i < n; i++ {
		tss = append(tss, GetTimeSeries())
	}
	return tss
}

func PutTimeSeriesSlice(tss []*TimeSeries) {
	for _, ts := range tss {
		PutTimeSeries(ts)
	}
}

func PutTimeSeries(ts *TimeSeries) {
	OutStandingTimeSeries.Dec()
	ts.Labels = ts.Labels[:0]
	ts.TS = 0
	ts.Value = 0
	ts.Hash = 0
	ts.Histogram = nil
	ts.FloatHistogram = nil
	tsPool.Put(ts)
}
func (ts *TimeSeries) AddLabels(lbls labels.Labels) {
	if cap(ts.Labels) < len(lbls) {
		ts.Labels = make([]Label, len(lbls))
	} else {
		ts.Labels = ts.Labels[:len(lbls)]
	}
	for i, l := range lbls {
		ts.Labels[i].Name = l.Name
		ts.Labels[i].Value = l.Value
	}
}

type TimeSeries struct {
	Labels         []Label         `cbor:"1,keyasint"`
	TS             int64           `cbor:"2,keyasint"`
	Value          float64         `cbor:"3,keyasint"`
	Hash           uint64          `cbor:"4,keyasint"`
	Histogram      *Histogram      `cbor:"5,keyasint"`
	FloatHistogram *FloatHistogram `cbor:"6,keyasint"`
}
type Label struct {
	Name  string
	Value string
}

func (ts TimeSeries) ByteLength() int {
	length := 0
	// Hash, value , TS
	length += 8 + 8 + 8
	for _, l := range ts.Labels {
		length += len(l.Name)
		length += len(l.Value)
	}
	return length
}

type MetaSeries struct {
	TimeSeries
}
*/

func defaultArgs() Arguments {
	return Arguments{
		TTL:               2 * time.Hour,
		MaxFlushSize:      10_000,
		FlushDuration:     5 * time.Second,
		AppenderBatchSize: 1_000,
	}
}

type Arguments struct {
	// TTL is how old a series can be.
	TTL time.Duration `alloy:"ttl,attr,optional"`
	// The batch size to persist to the file queue.
	MaxFlushSize int `alloy:"max_flush_size,attr,optional"`
	// How often to flush to the file queue if BatchSizeBytes isn't met.
	FlushDuration time.Duration      `alloy:"flush_duration,attr,optional"`
	Connections   []ConnectionConfig `alloy:"endpoint,block"`
	// AppenderBatchSize determines how often to flush the appender batch size.
	AppenderBatchSize int               `alloy:"appender_batch_size,attr,optional"`
	ExternalLabels    map[string]string `alloy:"external_labels,attr,optional"`
}

type ConnectionConfig struct {
	Name      string        `alloy:",label"`
	URL       string        `alloy:"url,attr"`
	BasicAuth BasicAuth     `alloy:"basic_auth,block,optional"`
	Timeout   time.Duration `alloy:"write_timeout,attr,optional"`
	// How long to wait between retries.
	RetryBackoff time.Duration `alloy:"retry_backoff,attr,optional"`
	// Maximum number of retries.
	MaxRetryBackoffAttempts int `alloy:"max_retry_backoff,attr,optional"`
	// How many series to write at a time.
	BatchCount int `alloy:"batch_count,attr,optional"`
	// How long to wait before sending regardless of batch count.
	FlushDuration time.Duration `alloy:"flush_duration,attr,optional"`
	// How many concurrent queues to have.
	QueueCount uint `alloy:"queue_count,attr,optional"`

	ExternalLabels map[string]string `alloy:"external_labels,attr,optional"`
}

type BasicAuth struct {
	Username string            `alloy:"username,attr,optional"`
	Password alloytypes.Secret `alloy:"password,attr,optional"`
}

type Exports struct {
	Receiver storage.Appendable `alloy:"receiver,attr"`
}

// SetToDefault sets the default
func (rc *Arguments) SetToDefault() {
	*rc = defaultArgs()
}
func defaultCC() ConnectionConfig {
	return ConnectionConfig{
		Timeout:                 15 * time.Second,
		RetryBackoff:            1 * time.Second,
		MaxRetryBackoffAttempts: 0,
		BatchCount:              1_000,
		FlushDuration:           1 * time.Second,
		QueueCount:              4,
	}
}
func (cc *ConnectionConfig) SetToDefault() {
	*cc = defaultCC()
}

func (r *Arguments) Validate() error {
	if r.AppenderBatchSize == 0 {
		return fmt.Errorf("appender_batch_size must be greater than zero")
	}
	for _, conn := range r.Connections {
		if conn.BatchCount <= 0 {
			return fmt.Errorf("batch_count must be greater than 0")
		}
	}
	return nil
}
