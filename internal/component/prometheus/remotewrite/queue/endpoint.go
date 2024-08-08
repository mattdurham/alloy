package queue

import (
	"context"
	"sync"
	"time"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/cbor"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"go.uber.org/atomic"
)

type endpoint struct {
	mut        sync.RWMutex
	fq         types.FileStorage
	client     types.NetworkClient
	serializer *cbor.Serializer
	stat       *types.PrometheusStats
	metaStats  *types.PrometheusStats
	log        log.Logger
	ctx        context.Context
	ttl        time.Duration
	done       atomic.Bool
}

func (ep *endpoint) runloop(ctx context.Context) {
	buf := make([]byte, 0)
	var name string
	var err error
	for {
		// If the endpoint is done then we should exit.
		if ep.done.Load() {
			return
		}
		_, buf, name, err = ep.fq.Next(ctx, buf)
		if err != nil {
			level.Error(ep.log).Log("msg", "error getting next file", "err", err)
			continue
		}

		buf, err = snappy.Decode(buf)
		if err != nil {
			level.Debug(ep.log).Log("msg", "error snappy decoding", "name", name, "err", err)
			continue
		}
		sg, err := cbor.DeserializeToSeriesGroup(buf)
		if err != nil {
			level.Debug(ep.log).Log("msg", "error deserializing", "name", name, "err", err)
			continue
		}
		func() {
			ep.mut.RLock()
			defer ep.mut.RUnlock()

			for _, series := range sg.Series {
				// One last chance to check the TTL. Writing to the filequeue will check it but
				// in a situation where the network is down and writing backs up we dont want to send
				// data that will get rejected.
				old := time.Since(time.Unix(series.TS, 0))
				if old > ep.ttl {
					continue
				}
				successful := ep.client.Queue(ctx, series.Hash, series.Bytes)
				if !successful {
					return
				}

			}
			for _, md := range sg.Metadata {
				successful := ep.client.QueueMetadata(ctx, md.Bytes)
				if !successful {
					return
				}
			}
		}()
	}
}

func (ep *endpoint) stop() {
	ep.done.Store(true)
}

var _ storage.Appender = (*fanout)(nil)

type fanout struct {
	children []storage.Appender
}

func (f fanout) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	for _, child := range f.children {
		_, err := child.Append(ref, l, t, v)
		if err != nil {
			return ref, err
		}
	}
	return ref, nil
}

func (f fanout) Commit() error {
	for _, child := range f.children {
		err := child.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (f fanout) Rollback() error {
	for _, child := range f.children {
		err := child.Rollback()
		if err != nil {
			return err
		}
	}
	return nil
}

func (f fanout) AppendExemplar(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	for _, child := range f.children {
		_, err := child.AppendExemplar(ref, l, e)
		if err != nil {
			return ref, err
		}
	}
	return ref, nil
}

func (f fanout) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	for _, child := range f.children {
		_, err := child.AppendHistogram(ref, l, t, h, fh)
		if err != nil {
			return ref, err
		}
	}
	return ref, nil
}

func (f fanout) UpdateMetadata(ref storage.SeriesRef, l labels.Labels, m metadata.Metadata) (storage.SeriesRef, error) {
	for _, child := range f.children {
		_, err := child.UpdateMetadata(ref, l, m)
		if err != nil {
			return ref, err
		}
	}
	return ref, nil
}

func (f fanout) AppendCTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64) (storage.SeriesRef, error) {
	for _, child := range f.children {
		_, err := child.AppendCTZeroSample(ref, l, t, ct)
		if err != nil {
			return ref, err
		}
	}
	return ref, nil
}
