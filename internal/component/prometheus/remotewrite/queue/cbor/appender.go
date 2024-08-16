package cbor

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
)

type appender struct {
	ttl       time.Duration
	s         types.Serializer
	data      []*types.TimeSeries
	metadata  []*types.MetaSeries
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
		ttl:       ttl,
		s:         s,
		data:      make([]*types.TimeSeries, 0),
		metadata:  make([]*types.MetaSeries, 0),
		logger:    logger,
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

	hash := l.Hash()
	ts := types.GetTimeSeries()
	ts.TS = t
	ts.Hash = hash
	ts.AddLabels(l)
	ts.Value = v
	a.data = append(a.data, ts)

	// Finally if we have enough data in the batch then send it to the serializer.
	// Originally we fed it each entry but there was some mutex overhead in highly concurrent scraping
	// Batching solved that problem, a batch size of 100 is enough.
	if len(a.data) >= a.batchSize {
		err := a.s.SendSeries(context.Background(), a.data)
		if err != nil {
			return ref, err
		}
		a.data = make([]*types.TimeSeries, 0)
	}
	a.stats(types.FileQueueStats{
		SeriesStored:    1,
		NewestTimestamp: t,
	})
	return ref, nil
}

// Commit is a no op since we always write.
func (a *appender) Commit() (_ error) {
	err := a.s.SendSeries(context.Background(), a.data)
	if err != nil {
		return err
	}
	return a.s.SendMetadata(context.Background(), a.metadata)
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
	hash := l.Hash()
	ts := types.GetTimeSeries()
	ts.Hash = hash
	ts.TS = ex.Timestamp
	ts.AddLabels(l)
	a.data = append(a.data, ts)
	return ref, nil
}

// AppendHistogram appends histogram
func (a *appender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (_ storage.SeriesRef, _ error) {
	endTime := time.Now().UTC().Unix() - int64(a.ttl.Seconds())
	if t < endTime {
		return ref, nil
	}
	ts := types.GetTimeSeries()

	if h != nil {
		ts.FromHistogram(t, h)
	} else {
		ts.FromFlotHistogram(t, fh)
	}
	ts.AddLabels(l)
	ts.Hash = l.Hash()
	ts.TS = t

	a.data = append(a.data, ts)
	a.stats(types.FileQueueStats{
		SeriesStored:    1,
		NewestTimestamp: t,
	})
	return ref, nil
}

// UpdateMetadata updates metadata.
func (a *appender) UpdateMetadata(ref storage.SeriesRef, l labels.Labels, m metadata.Metadata) (_ storage.SeriesRef, _ error) {
	return 0, nil
	/*
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
		a.data = append(a.data, types.TimeSeries{
			Hash:   hash,
			TS:     ex.Timestamp,
			Labels: l,
		})
		return ref, nil
	*/

}
