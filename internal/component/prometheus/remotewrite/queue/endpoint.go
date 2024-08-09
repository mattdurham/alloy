package queue

import (
	"context"
	"time"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/vladopajic/go-actor/actor"
)

var _ actor.Worker = (*endpoint)(nil)

type endpoint struct {
	client     types.NetworkClient
	serializer types.Serializer
	stat       *types.PrometheusStats
	metaStats  *types.PrometheusStats
	log        log.Logger
	ctx        context.Context
	ttl        time.Duration
	mbx        actor.Mailbox[types.DataHandle]
	buf        []byte
	self       actor.Actor
}

func (ep *endpoint) Start() {
	ep.self = actor.Combine(actor.New(ep), ep.mbx).Build()
	ep.self.Start()
}

func (ep *endpoint) Stop() {
	ep.client.Stop()
	ep.self.Stop()
}

func (ep *endpoint) DoWork(ctx actor.Context) actor.WorkerStatus {
	select {
	case <-ctx.Done():
		return actor.WorkerEnd
	case item, ok := <-ep.mbx.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		_, buf, err := item.Get(item.Name)
		if err != nil {
			return actor.WorkerEnd
		}
		ep.handleItem(buf)
		return actor.WorkerContinue
	}
}

func (ep *endpoint) handleItem(buf []byte) {
	var err error
	ep.buf, err = snappy.Decode(buf)
	if err != nil {
		level.Debug(ep.log).Log("msg", "error snappy decoding", "err", err)
		return
	}
	sg, err := types.DeserializeToSeriesGroup(ep.buf)
	if err != nil {
		level.Debug(ep.log).Log("msg", "error deserializing", "err", err)
		return
	}

	for _, series := range sg.Series {
		// One last chance to check the TTL. Writing to the filequeue will check it but
		// in a situation where the network is down and writing backs up we dont want to send
		// data that will get rejected.
		old := time.Since(time.Unix(series.TS, 0))
		if old > ep.ttl {
			continue
		}
		sendErr := ep.client.SendSeries(context.Background(), series.Hash, series.Bytes)
		if sendErr != nil {
			level.Error(ep.log).Log("msg", "error sending to write client", "err", sendErr)
		}

	}
	for _, md := range sg.Metadata {
		sendErr := ep.client.SendMetadata(context.Background(), md.Bytes)
		if sendErr != nil {
			level.Error(ep.log).Log("msg", "error sending metadata to write client", "err", sendErr)
		}
	}
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
