package serialization

import (
	"context"
	"strconv"
	"time"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/go-kit/log"
	"github.com/grafana/alloy/internal/component/prometheus/remote/queue/types"
	"github.com/vladopajic/go-actor/actor"
)

// serializer collects data from multiple appenders and will write them to file.Storage.
// serializer will trigger based on the last flush duration OR if it hits a certain amount of items.
type serializer struct {
	inbox               actor.Mailbox[*types.TimeSeriesBinary]
	metaInbox           actor.Mailbox[*types.TimeSeriesBinary]
	maxItemsBeforeFlush int
	flushDuration       time.Duration
	queue               types.FileStorage
	lastFlush           time.Time
	logger              log.Logger
	self                actor.Actor
	flushTestTimer      *time.Ticker
	series              []*types.TimeSeriesBinary
	meta                []*types.TimeSeriesBinary
	msgpBuffer          []byte
}

func NewSerializer(maxItemsBeforeFlush int, flushDuration time.Duration, q types.FileStorage, l log.Logger) (types.Serializer, error) {
	s := &serializer{
		maxItemsBeforeFlush: maxItemsBeforeFlush,
		flushDuration:       flushDuration,
		queue:               q,
		series:              make([]*types.TimeSeriesBinary, 0),
		logger:              l,
		inbox:               actor.NewMailbox[*types.TimeSeriesBinary](),
		metaInbox:           actor.NewMailbox[*types.TimeSeriesBinary](),
		flushTestTimer:      time.NewTicker(1 * time.Second),
		msgpBuffer:          make([]byte, 0),
		lastFlush:           time.Now(),
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

func (s *serializer) SendSeries(ctx context.Context, data *types.TimeSeriesBinary) error {
	return s.inbox.Send(ctx, data)
}

func (s *serializer) SendMetadata(ctx context.Context, data *types.TimeSeriesBinary) error {
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

func (s *serializer) AppendMetadata(ctx actor.Context, data *types.TimeSeriesBinary) error {

	s.meta = append(s.meta, data)
	// If we would go over the max size then send, or if we have hit the flush duration then send.
	if len(s.meta) >= s.maxItemsBeforeFlush {
		return s.store(ctx)
	} else if time.Since(s.lastFlush) > s.flushDuration {
		return s.store(ctx)
	}
	return nil
}

func (s *serializer) Append(ctx actor.Context, data *types.TimeSeriesBinary) error {

	s.series = append(s.series, data)
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
	group := &types.SeriesGroup{
		Series:   make([]*types.TimeSeriesBinary, len(s.series)),
		Metadata: make([]*types.TimeSeriesBinary, len(s.meta)),
	}
	defer func() {
		types.PutTimeSeriesBinarySlice(s.series)
		s.series = make([]*types.TimeSeriesBinary, 0)
	}()

	strMapToInt := make(map[string]int32)
	index := int32(0)

	for si, ser := range s.series {
		index = types.FillBinary(ser, strMapToInt, index)
		group.Series[si] = ser
	}
	stringsSlice := make([]string, len(strMapToInt))
	for k, v := range strMapToInt {
		stringsSlice[v] = k
	}
	group.Strings = stringsSlice

	buf, err := group.MarshalMsg(s.msgpBuffer)
	if err != nil {
		return err
	}

	out := snappy.Encode(buf)
	meta := map[string]string{
		// product.signal_type.schema.version
		"version":       "alloy.metrics.queue.v1",
		"encoding":      "snappy",
		"series_count":  strconv.Itoa(len(group.Series)),
		"meta_count":    strconv.Itoa(len(group.Metadata)),
		"strings_count": strconv.Itoa(len(group.Strings)),
	}
	err = s.queue.Send(ctx, meta, out)
	return err
}
