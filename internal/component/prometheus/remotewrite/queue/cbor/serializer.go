package cbor

import (
	"context"
	"time"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/fxamacker/cbor/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/vladopajic/go-actor/actor"
)

type Serializer struct {
	inbox         actor.Mailbox[[]types.Raw]
	metaInbox     actor.Mailbox[[]types.RawMetadata]
	maxSizeBytes  int
	flushDuration time.Duration
	queue         types.FileStorage
	group         *types.SeriesGroup
	lastFlush     time.Time
	bytesInGroup  uint32
	logger        log.Logger
	self          actor.Actor
}

func NewSerializer(maxSizeBytes int, flushDuration time.Duration, q types.FileStorage, l log.Logger) (types.Serializer, error) {
	s := &Serializer{
		maxSizeBytes:  maxSizeBytes,
		flushDuration: flushDuration,
		queue:         q,
		group: &types.SeriesGroup{
			Series:   make([]types.Raw, 0),
			Metadata: make([]types.Raw, 0),
		},
		logger:    l,
		inbox:     actor.NewMailbox[[]types.Raw](),
		metaInbox: actor.NewMailbox[[]types.RawMetadata](),
	}

	return s, nil
}

func (s *Serializer) Start() {
	s.self = actor.Combine(actor.New(s), s.inbox, s.metaInbox).Build()
	s.self.Start()
}

func (s *Serializer) Stop() {
	s.self.Stop()
}

func (s *Serializer) SendSeries(ctx context.Context, data []types.Raw) error {
	return s.inbox.Send(ctx, data)
}

func (s *Serializer) SendMetadata(ctx context.Context, data []types.RawMetadata) error {
	return s.metaInbox.Send(ctx, data)
}

func (s *Serializer) Mailbox() actor.MailboxSender[[]types.Raw] {
	return s.inbox
}

func (s *Serializer) MetaMailbox() actor.MailboxSender[[]types.RawMetadata] {
	return s.metaInbox
}

func (s *Serializer) DoWork(ctx actor.Context) actor.WorkerStatus {

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
	}
}

func (s *Serializer) AppendMetadata(ctx actor.Context, data []types.RawMetadata) error {
	if len(data) == 0 {
		return nil
	}

	for _, d := range data {
		s.group.Metadata = append(s.group.Series, d.Raw)
		s.bytesInGroup = s.bytesInGroup + uint32(len(d.Bytes)) + 4
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

func (s *Serializer) Append(ctx actor.Context, data []types.Raw) error {
	if len(data) == 0 {
		return nil
	}

	for _, d := range data {
		s.group.Series = append(s.group.Series, d)
		s.bytesInGroup = s.bytesInGroup + uint32(len(d.Bytes)) + 4
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

var version = map[string]string{
	// product.signal_type.schema.version
	"version":  "alloy.metrics.simple.v1",
	"encoding": "snappy",
}

func (s *Serializer) store(ctx actor.Context) error {
	s.lastFlush = time.Now()

	buffer, err := cbor.Marshal(s.group)
	// We can reset the group now.
	s.group.Series = s.group.Series[:0]
	s.group.Metadata = s.group.Metadata[:0]

	if err != nil {
		// Something went wrong with serializing the whole group so lets drop it.
		return err
	}
	out := snappy.Encode(buffer)
	err = s.queue.Send(ctx, version, out)
	s.bytesInGroup = 0
	return err
}
