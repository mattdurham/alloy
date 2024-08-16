package cbor

import (
	"context"
	"strconv"
	"time"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/vladopajic/go-actor/actor"
)

type Serializer struct {
	inbox          actor.Mailbox[[]*types.TimeSeries]
	metaInbox      actor.Mailbox[[]*types.MetaSeries]
	maxSizeBytes   int
	flushDuration  time.Duration
	queue          types.FileStorage
	lastFlush      time.Time
	bytesInGroup   uint32
	logger         log.Logger
	self           actor.Actor
	flushTestTimer *time.Ticker
	series         []*types.TimeSeries
	meta           []*types.MetaSeries
	stringList     []string
}

func NewSerializer(maxSizeBytes int, flushDuration time.Duration, q types.FileStorage, l log.Logger) (types.Serializer, error) {
	s := &Serializer{
		maxSizeBytes:   maxSizeBytes,
		flushDuration:  flushDuration,
		queue:          q,
		series:         make([]*types.TimeSeries, 0),
		logger:         l,
		inbox:          actor.NewMailbox[[]*types.TimeSeries](),
		metaInbox:      actor.NewMailbox[[]*types.MetaSeries](),
		flushTestTimer: time.NewTicker(1 * time.Second),
	}

	return s, nil
}

func (s *Serializer) Start() {
	s.queue.Start()
	s.self = actor.Combine(actor.New(s), s.inbox, s.metaInbox).Build()
	s.self.Start()
}

func (s *Serializer) Stop() {
	s.queue.Stop()
	s.self.Stop()
}

func (s *Serializer) SendSeries(ctx context.Context, data []*types.TimeSeries) error {
	return s.inbox.Send(ctx, data)
}

func (s *Serializer) SendMetadata(ctx context.Context, data []*types.MetaSeries) error {
	return s.metaInbox.Send(ctx, data)
}
func (s *Serializer) DoWork(ctx actor.Context) actor.WorkerStatus {
	select {
	case <-ctx.Done():
		return actor.WorkerEnd
	case item, ok := <-s.inbox.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		level.Debug(s.logger).Log("msg", "received item", "len", len(item))
		s.Append(ctx, item)
		return actor.WorkerContinue
	case item, ok := <-s.metaInbox.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		s.AppendMetadata(ctx, item)
		return actor.WorkerContinue
	case <-s.flushTestTimer.C:
		if time.Since(s.lastFlush) > s.flushDuration {
			s.store(ctx)
		}
		return actor.WorkerContinue
	}
}

func (s *Serializer) AppendMetadata(ctx actor.Context, data []*types.MetaSeries) error {
	if len(data) == 0 {
		return nil
	}

	for _, d := range data {
		s.meta = append(s.meta, d)
		s.bytesInGroup = s.bytesInGroup + uint32(d.ByteLength()) + 4
	}
	// If we would go over the max size then send, or if we have hit the flush duration then send.
	if s.bytesInGroup > uint32(s.maxSizeBytes) {
		level.Debug(s.logger).Log("flushing to disk due to maxSizeBytes", s.maxSizeBytes)
		return s.store(ctx)
	} else if time.Since(s.lastFlush) > s.flushDuration {
		return s.store(ctx)
	}
	return nil
}

func (s *Serializer) Append(ctx actor.Context, data []*types.TimeSeries) error {
	if len(data) == 0 {
		return nil
	}

	for _, d := range data {
		s.series = append(s.series, d)
		s.bytesInGroup = s.bytesInGroup + uint32(d.ByteLength()) + 4
	}
	// If we would go over the max size then send, or if we have hit the flush duration then send.
	if s.bytesInGroup > uint32(s.maxSizeBytes) {
		level.Debug(s.logger).Log("flushing to disk due to maxSizeBytes", s.maxSizeBytes)
		return s.store(ctx)
	} else if time.Since(s.lastFlush) > s.flushDuration {
		level.Debug(s.logger).Log("flushing to disk due to flush timer", s.maxSizeBytes)
		return s.store(ctx)
	}
	return nil
}

func (s *Serializer) store(ctx actor.Context) error {
	s.lastFlush = time.Now()
	if len(s.series) == 0 && len(s.meta) == 0 {
		return nil
	}
	s.stringList = s.stringList[:0]
	group := &types.SeriesGroup{
		Series:   make([]types.TimeSeriesBinary, len(s.series)),
		Metadata: make([]types.MetaSeriesBinary, len(s.meta)),
	}
	defer func() {
		types.PutTimeSeriesSlice(s.series)
		s.series = make([]*types.TimeSeries, 0)
	}()

	strMapToInt := make(map[string]int32)
	index := int32(0)
	defer func() {
		types.PutTimeSeriesBinarySliceValue(group.Series)
	}()

	for si, ser := range s.series {
		// You may ask why we arent using a pool? Because since this is only used in the function
		// it stays in the stack and not the heap. Which means GC doesnt kick in.
		ts := types.GetTimeSeriesBinary()
		if cap(ts.LabelsNames) < len(ser.Labels) {
			ts.LabelsNames = make([]int32, len(ser.Labels))
		} else {
			ts.LabelsNames = ts.LabelsNames[:len(ser.Labels)]
		}
		if cap(ts.LabelsValues) < len(ser.Labels) {
			ts.LabelsValues = make([]int32, len(ser.Labels))
		} else {
			ts.LabelsValues = ts.LabelsValues[:len(ser.Labels)]
		}
		ts.TS = ser.TS
		ts.Value = ser.Value
		ts.Hash = ser.Hash

		ts.Histograms.Histogram = ser.Histogram
		ts.Histograms.FloatHistogram = ser.FloatHistogram

		for i, v := range ser.Labels {
			val, found := strMapToInt[v.Name]
			if !found {
				strMapToInt[v.Name] = index
				val = index
				index++
			}
			ts.LabelsNames[i] = val

			val, found = strMapToInt[v.Value]
			if !found {
				strMapToInt[v.Value] = index
				val = index
				index++
			}
			ts.LabelsValues[i] = val
		}
		group.Series[si] = *ts
	}
	stringsSlice := make([]string, len(strMapToInt))
	for k, v := range strMapToInt {
		stringsSlice[v] = k
	}

	group.Strings = stringsSlice
	buf, err := group.MarshalMsg(nil)
	if err != nil {
		return err
	}

	out := snappy.Encode(buf)
	meta := map[string]string{
		// product.signal_type.schema.version
		"version":      "alloy.metrics.simple.v1",
		"encoding":     "snappy",
		"series_count": strconv.Itoa(len(group.Series)),
		"meta_count":   strconv.Itoa(len(group.Metadata)),
	}
	err = s.queue.Send(ctx, meta, out)
	s.bytesInGroup = 0
	return err
}
