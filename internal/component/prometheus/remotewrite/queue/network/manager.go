package network

import (
	"context"
	"github.com/grafana/alloy/internal/runtime/logging/level"

	"github.com/go-kit/log"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/vladopajic/go-actor/actor"
)

type manager struct {
	connectionCount uint64
	loops           []*loop
	metadata        *loop
	logger          log.Logger
	inbox           actor.Mailbox[types.NetworkQueueItem]
	metaInbox       actor.Mailbox[types.NetworkMetadataItem]
	self            actor.Actor
	cfg             ConnectionConfig
	stats           func(types.NetworkStats)
	metaStats       func(types.NetworkStats)
}

var _ types.NetworkClient = (*manager)(nil)

var _ actor.Worker = (*manager)(nil)

func New(cc ConnectionConfig, logger log.Logger, seriesStats, metadataStats func(types.NetworkStats)) (types.NetworkClient, error) {
	s := &manager{
		connectionCount: cc.Connections,
		loops:           make([]*loop, 0),
		logger:          logger,
		inbox:           actor.NewMailbox[types.NetworkQueueItem](actor.OptCapacity(1)),
		metaInbox:       actor.NewMailbox[types.NetworkMetadataItem](actor.OptCapacity(1)),
		stats:           seriesStats,
	}

	// start kicks off a number of concurrent connections.
	var i uint64
	for ; i < s.connectionCount; i++ {
		l := newLoop(cc, logger, seriesStats)
		l.self = actor.New(l)
		s.loops = append(s.loops, l)
	}

	s.metadata = newLoop(cc, logger, metadataStats)
	s.metadata.self = actor.New(s.metadata)
	return s, nil
}

func (s *manager) Start() {
	actors := make([]actor.Actor, 0)
	for _, l := range s.loops {
		l.Start()
	}
	actors = append(actors, s.metadata.actors()...)
	actors = append(actors, s.inbox)
	actors = append(actors, s.metaInbox)
	actors = append(actors, actor.New(s))
	s.self = actor.Combine(actors...).Build()
	s.self.Start()
}

func (s *manager) SendSeries(ctx context.Context, hash uint64, data *types.TimeSeries) error {
	return s.inbox.Send(ctx, types.NetworkQueueItem{
		Hash: hash,
		TS:   data,
	})
}

func (s *manager) SendMetadata(ctx context.Context, data *types.MetaSeries) error {
	return s.metaInbox.Send(ctx, types.NetworkMetadataItem{
		TS: data,
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
		s.Queue(ctx, item.Hash, item.TS)
		return actor.WorkerContinue
	case _, ok := <-s.metaInbox.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		//s.QueueMetadata(ctx, item.Buffer)
		return actor.WorkerContinue
	}
}

func (s *manager) updateConfig(cc ConnectionConfig) {
	// No need to do anything if the configuration is the same.
	if s.cfg.Equals(cc) {
		return
	}
	// TODO @mattdurham make this smarter.

	// For the moment we will stop all the items and recreate them.
	for _, l := range s.loops {
		l.Stop()
	}
	s.metadata.Stop()

	s.loops = make([]*loop, 0)
	var i uint64
	for ; i < s.connectionCount; i++ {
		l := newLoop(cc, s.logger, s.stats)
		l.self = actor.New(l)
		s.loops = append(s.loops, l)
	}

	s.metadata = newLoop(cc, s.logger, s.metaStats)
	s.metadata.self = actor.New(s.metadata)
}

func (s *manager) Stop() {
	level.Debug(s.logger).Log("msg", "stopping manager")
	for _, l := range s.loops {
		l.Stop()
		l.stopCalled.Store(true)
	}
	s.metadata.stopCalled.Store(true)
	s.self.Stop()
}

// Queue adds anything thats not metadata to the queue.
func (s *manager) Queue(ctx context.Context, hash uint64, d *types.TimeSeries) {
	// Based on a hash which is the label hash add to the queue.
	queueNum := hash % s.connectionCount
	s.loops[queueNum].seriesMbx.Send(ctx, d)
}

// QueueMetadata adds metadata to the queue.
func (s *manager) QueueMetadata(ctx context.Context, d *types.MetaSeries) {
	s.metadata.metaSeriesMbx.Send(ctx, d)
}
