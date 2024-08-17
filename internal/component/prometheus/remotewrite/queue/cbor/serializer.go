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

// serializer collects data from multiple appenders and will write them to file.Storage.
// serializer will trigger based on the last flush duration OR if it hits a certain amount of items.
type serializer struct {
	inbox               actor.Mailbox[[]*types.TimeSeries]
	metaInbox           actor.Mailbox[[]*types.MetaSeries]
	maxItemsBeforeFlush int
	flushDuration       time.Duration
	queue               types.FileStorage
	lastFlush           time.Time
	logger              log.Logger
	self                actor.Actor
	flushTestTimer      *time.Ticker
	series              []*types.TimeSeries
	meta                []*types.MetaSeries
	stringList          []string
}

func NewSerializer(maxItemsBeforeFlush int, flushDuration time.Duration, q types.FileStorage, l log.Logger) (types.Serializer, error) {
	s := &serializer{
		maxItemsBeforeFlush: maxItemsBeforeFlush,
		flushDuration:       flushDuration,
		queue:               q,
		series:              make([]*types.TimeSeries, 0),
		logger:              l,
		inbox:               actor.NewMailbox[[]*types.TimeSeries](),
		metaInbox:           actor.NewMailbox[[]*types.MetaSeries](),
		flushTestTimer:      time.NewTicker(1 * time.Second),
	}

	return s, nil
}

func (s *serializer) Start() {
	s.queue.Start()
	s.self = actor.Combine(actor.New(s), s.inbox, s.metaInbox).Build()
	s.self.Start()
}

func (s *serializer) Stop() {
	s.queue.Stop()
	s.self.Stop()
}

func (s *serializer) SendSeries(ctx context.Context, data []*types.TimeSeries) error {
	return s.inbox.Send(ctx, data)
}

func (s *serializer) SendMetadata(ctx context.Context, data []*types.MetaSeries) error {
	return s.metaInbox.Send(ctx, data)
}
func (s *serializer) DoWork(ctx actor.Context) actor.WorkerStatus {
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

func (s *serializer) AppendMetadata(ctx actor.Context, data []*types.MetaSeries) error {
	if len(data) == 0 {
		return nil
	}

	s.meta = append(s.meta, data...)
	// If we would go over the max size then send, or if we have hit the flush duration then send.
	if len(s.meta) >= s.maxItemsBeforeFlush {
		return s.store(ctx)
	} else if time.Since(s.lastFlush) > s.flushDuration {
		return s.store(ctx)
	}
	return nil
}

func (s *serializer) Append(ctx actor.Context, data []*types.TimeSeries) error {
	if len(data) == 0 {
		return nil
	}

	s.series = append(s.series, data...)
	// If we would go over the max size then send, or if we have hit the flush duration then send.
	if len(s.series) >= s.maxItemsBeforeFlush {
		return s.store(ctx)
	} else if time.Since(s.lastFlush) > s.flushDuration {
		return s.store(ctx)
	}
	return nil
}

func (s *serializer) store(ctx actor.Context) error {
	s.lastFlush = time.Now()
	if len(s.series) == 0 && len(s.meta) == 0 {
		return nil
	}
	s.stringList = s.stringList[:0]
	group := &types.SeriesGroup{
		Series:   make([]*types.TimeSeriesBinary, len(s.series)),
		Metadata: make([]*types.MetaSeriesBinary, len(s.meta)),
	}
	defer func() {
		types.PutTimeSeriesSlice(s.series)
		s.series = make([]*types.TimeSeries, 0)
	}()

	strMapToInt := make(map[string]int32)
	index := int32(0)
	defer func() {
		types.PutTimeSeriesBinarySlice(group.Series)
	}()

	for si, ser := range s.series {
		ts := types.GetTimeSeriesBinary()
		index = fillBinary(ts, ser, strMapToInt, index)
		group.Series[si] = ts
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
	return err
}

func fillBinary(ts *types.TimeSeriesBinary, ser *types.TimeSeries, strMapToInt map[string]int32, index int32) int32 {
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

}
