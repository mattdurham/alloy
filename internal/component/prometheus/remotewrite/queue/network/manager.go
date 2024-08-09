package network

import (
	"context"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/prometheus/prometheus/prompb"
	"github.com/vladopajic/go-actor/actor"
)

type manager struct {
	connectionCount uint64
	loops           []*loop
	metadata        *loop
	logger          log.Logger
	inbox           actor.Mailbox[types.NetworkQueueItem]
	metaInbox       actor.Mailbox[types.NetworkMetadataItem]
	combinedActor   actor.Actor
}

var _ types.NetworkClient = (*manager)(nil)

var _ actor.Worker = (*manager)(nil)

type ConnectionConfig struct {
	URL                     string
	Username                string
	Password                string
	UserAgent               string
	Timeout                 time.Duration
	RetryBackoff            time.Duration
	MaxRetryBackoffAttempts time.Duration
	BatchCount              int
	FlushDuration           time.Duration
	ExternalLabels          map[string]string
}

func New(ctx context.Context, cc ConnectionConfig, connectionCount uint64, logger log.Logger, seriesStats, metadataStats func(types.NetworkStats)) (types.NetworkClient, error) {
	s := &manager{
		connectionCount: connectionCount,
		loops:           make([]*loop, 0),
		logger:          logger,
		inbox:           actor.NewMailbox[types.NetworkQueueItem](actor.OptCapacity(1)),
		metaInbox:       actor.NewMailbox[types.NetworkMetadataItem](actor.OptCapacity(1)),
	}

	// start kicks off a number of concurrent connections.
	var i uint64
	for ; i < s.connectionCount; i++ {
		l := &loop{
			seriesMbx:      actor.NewMailbox[[]byte](actor.OptCapacity(2 * cc.BatchCount)),
			configMbx:      actor.NewMailbox[ConnectionConfig](),
			metaSeriesMbx:  actor.NewMailbox[[]byte](),
			batchCount:     cc.BatchCount,
			flushTimer:     cc.FlushDuration,
			client:         &http.Client{},
			cfg:            cc,
			pbuf:           proto.NewBuffer(nil),
			buf:            make([]byte, 0),
			log:            logger,
			seriesBuf:      make([]prompb.TimeSeries, 0),
			statsFunc:      seriesStats,
			externalLabels: cc.ExternalLabels,
		}
		l.self = actor.New(l)
		s.loops = append(s.loops, l)
	}

	s.metadata = &loop{
		seriesMbx:     actor.NewMailbox[[]byte](actor.OptCapacity(2 * cc.BatchCount)),
		configMbx:     actor.NewMailbox[ConnectionConfig](),
		metaSeriesMbx: actor.NewMailbox[[]byte](),
		batchCount:    cc.BatchCount,
		flushTimer:    cc.FlushDuration,
		client:        &http.Client{},
		cfg:           cc,
		pbuf:          proto.NewBuffer(nil),
		buf:           make([]byte, 0),
		log:           logger,
		seriesBuf:     make([]prompb.TimeSeries, 0),
		statsFunc:     metadataStats,
	}
	s.metadata.self = actor.New(s.metadata)
	return s, nil
}

func (s *manager) Start() {
	actors := make([]actor.Actor, 0)
	for _, l := range s.loops {
		actors = append(actors, l.actors()...)
	}
	actors = append(actors, s.metadata.actors()...)
	actors = append(actors, s.inbox)
	actors = append(actors, s.metaInbox)
	actors = append(actors, actor.New(s))

	s.combinedActor = actor.Combine(actors...).Build()
	s.combinedActor.Start()
}

func (s *manager) SendSeries(ctx context.Context, hash uint64, data []byte) error {
	return s.inbox.Send(ctx, types.NetworkQueueItem{
		Hash:   hash,
		Buffer: data,
	})
}

func (s *manager) SendMetadata(ctx context.Context, data []byte) error {
	return s.metaInbox.Send(ctx, types.NetworkMetadataItem{
		Buffer: data,
	})
}

func (s *manager) DoWork(ctx actor.Context) actor.WorkerStatus {
	select {
	case <-ctx.Done():
		s.Stop()
		return actor.WorkerEnd
	case item, ok := <-s.inbox.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		s.Queue(ctx, item.Hash, item.Buffer)
		return actor.WorkerContinue
	case item, ok := <-s.metaInbox.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		s.QueueMetadata(ctx, item.Buffer)
		return actor.WorkerContinue
	}
}

func (s *manager) Stop() {
	for _, l := range s.loops {
		l.stopCalled.Store(true)
	}
	s.metadata.stopCalled.Store(true)
	s.combinedActor.Stop()

}

// Queue adds anything thats not metadata to the queue.
func (s *manager) Queue(ctx context.Context, hash uint64, d []byte) {
	// Based on a hash which is the label hash add to the queue.
	queueNum := hash % s.connectionCount
	s.loops[queueNum].seriesMbx.Send(ctx, d)
}

// QueueMetadata adds metadata to the queue.
func (s *manager) QueueMetadata(ctx context.Context, d []byte) {
	s.metadata.metaSeriesMbx.Send(ctx, d)
}
