package cbor

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
)

type appender struct {
	ttl       time.Duration
	s         types.Serializer
	ts        *prompb.TimeSeries
	data      []*types.Raw
	metadata  []*types.RawMetadata
	logger    log.Logger
	batchSize int
	stats     func(s types.FileQueueStats)
}

func (a *appender) AppendCTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64) (storage.SeriesRef, error) {
	// TODO @mattdurham figure out what to do here later.
	return ref, nil
}

// NewAppender returns an Appender that writes to a given serializer. NOTE the Appender returned writes
// data immediately and does not honor commit or rollback.
func NewAppender(ttl time.Duration, s types.Serializer, batchSize int, stats func(s types.FileQueueStats), logger log.Logger) storage.Appender {
	app := &appender{
		ttl:      ttl,
		s:        s,
		data:     make([]*types.Raw, 0),
		metadata: make([]*types.RawMetadata, 0),
		logger:   logger,
		ts: &prompb.TimeSeries{
			Labels:     make([]prompb.Label, 0),
			Samples:    make([]prompb.Sample, 0),
			Exemplars:  make([]prompb.Exemplar, 0),
			Histograms: make([]prompb.Histogram, 0),
		},
		batchSize: batchSize,
		stats:     stats,
	}
	return app
}

// Append metric
func (a *appender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	// Check to see if the TTL has expired for this record.
	endTime := time.Now().UTC().Unix() - int64(a.ttl.Seconds())
	if t < endTime {
		return ref, nil
	}

	for _, l := range l {
		a.ts.Labels = append(a.ts.Labels, prompb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	a.ts.Samples = append(a.ts.Samples, prompb.Sample{
		Value:     v,
		Timestamp: t,
	})
	data, err := a.ts.Marshal()
	if err != nil {
		return ref, err
	}
	hash := l.Hash()
	a.data = append(a.data, &types.Raw{
		Hash:  hash,
		TS:    t,
		Bytes: data,
	})
	// Finally if we have enough data in the batch then send it to the serializer.
	// Originally we fed it each entry but there was some mutex overhead in highly concurrent scraping
	// Batching solved that problem, a batch size of 100 is enough.
	if len(a.data) >= a.batchSize {
		err = a.s.Mailbox().Send(context.Background(), a.data)
		if err != nil {
			return ref, err
		}
		a.data = a.data[:0]
	}
	a.stats(types.FileQueueStats{
		SeriesStored:    1,
		NewestTimestamp: t,
	})
	a.resetTS()
	return ref, nil
}

// Commit is a no op since we always write.
func (a *appender) Commit() (_ error) {
	err := a.s.Mailbox().Send(context.Background(), a.data)
	if err != nil {
		return err
	}
	return a.s.MetaMailbox().Send(context.Background(), a.metadata)
}

// Rollback is a no op since we write all the data.
func (a *appender) Rollback() error {
	return nil
}

// AppendExemplar appends exemplar to cache.
func (a *appender) AppendExemplar(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (_ storage.SeriesRef, _ error) {
	endTime := time.Now().UTC().Unix() - int64(a.ttl.Seconds())
	if e.HasTs && e.Ts < endTime {
		return ref, nil
	}
	ex := prompb.Exemplar{}
	ex.Value = e.Value
	ex.Timestamp = e.Ts
	for _, l := range l {
		ex.Labels = append(a.ts.Labels, prompb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	a.ts.Exemplars = append(a.ts.Exemplars, ex)
	data, err := a.ts.Marshal()
	if err != nil {
		return ref, err
	}
	hash := l.Hash()
	a.data = append(a.data, &types.Raw{
		Hash:  hash,
		TS:    ex.Timestamp,
		Bytes: data,
	})

	a.resetTS()
	return ref, nil
}

// AppendHistogram appends histogram
func (a *appender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (_ storage.SeriesRef, _ error) {
	endTime := time.Now().UTC().Unix() - int64(a.ttl.Seconds())
	if t < endTime {
		return ref, nil
	}
	for _, l := range l {
		a.ts.Labels = append(a.ts.Labels, prompb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	if h != nil {
		a.ts.Histograms = append(a.ts.Histograms, remote.HistogramToHistogramProto(t, h))
	} else {
		a.ts.Histograms = append(a.ts.Histograms, remote.FloatHistogramToHistogramProto(t, fh))
	}
	data, err := a.ts.Marshal()
	if err != nil {
		return ref, err
	}
	hash := l.Hash()
	a.data = append(a.data, &types.Raw{
		Hash:  hash,
		TS:    t,
		Bytes: data,
	})
	a.stats(types.FileQueueStats{
		SeriesStored:    1,
		NewestTimestamp: t,
	})
	a.resetTS()
	return ref, nil
}

// UpdateMetadata updates metadata.
func (a *appender) UpdateMetadata(ref storage.SeriesRef, l labels.Labels, m metadata.Metadata) (_ storage.SeriesRef, _ error) {
	var name string
	for _, lbl := range l {
		if lbl.Name == "__name__" {
			name = lbl.Name
			break
		}
	}
	if name == "" {
		return ref, fmt.Errorf("unable to find name for metadata")
	}
	md := prompb.MetricMetadata{
		Type: prompb.MetricMetadata_MetricType(prompb.MetricMetadata_MetricType_value[string(m.Type)]),
		Help: m.Help,
		Unit: m.Unit,
	}
	md.MetricFamilyName = name
	data, err := md.Marshal()
	if err != nil {
		return ref, err
	}
	a.data = append(a.data, &types.Raw{
		Hash:  l.Hash(),
		TS:    0,
		Bytes: data,
	})
	return ref, nil
}

func (a *appender) resetTS() {
	a.ts.Labels = a.ts.Labels[:0]
	a.ts.Samples = a.ts.Samples[:0]
	a.ts.Exemplars = a.ts.Exemplars[:0]
	a.ts.Histograms = a.ts.Histograms[:0]
}
