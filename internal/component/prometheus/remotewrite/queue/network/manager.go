package network

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/prometheus/prometheus/prompb"
	"github.com/vladopajic/go-actor/actor"
)

type manager struct {
	mut             sync.Mutex
	connectionCount uint64
	loops           []actor.Actor
	loopsMbx        []actor.Mailbox[[]byte]
	metadata        actor.Actor
	metaMbx         actor.Mailbox[[]byte]
	logger          log.Logger
	inbox           actor.Mailbox[types.NetworkQueueItem]
	metaInbox       actor.Mailbox[types.NetworkMetadataItem]
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
		loops:           make([]actor.Actor, 0),
		loopsMbx:        make([]actor.Mailbox[[]byte], 0),
		logger:          logger,
		inbox:           actor.NewMailbox[types.NetworkQueueItem](),
	}

	// start kicks off a number of concurrent connections.
	var i uint64
	for ; i < s.connectionCount; i++ {
		mbx := actor.NewMailbox[[]byte](actor.OptCapacity(2 * cc.BatchCount))
		l := &loop{
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
			seriesMbx:      mbx,
		}
		mbx.Start()
		s.loopsMbx = append(s.loopsMbx, mbx)
		lActor := actor.New(l)
		s.loops = append(s.loops, lActor)
		lActor.Start()
	}

	s.metadata = actor.New(&loop{
		batchCount: cc.BatchCount,
		flushTimer: cc.FlushDuration,
		client:     &http.Client{},
		cfg:        cc,
		pbuf:       proto.NewBuffer(nil),
		buf:        make([]byte, 0),
		log:        logger,
		seriesBuf:  make([]prompb.TimeSeries, 0),
		statsFunc:  metadataStats,
		seriesMbx:  actor.NewMailbox[[]byte](),
	})
	s.inbox.Start()
	return s, nil
}

func (s *manager) Mailbox() actor.MailboxSender[types.NetworkQueueItem] {
	return s.inbox
}

func (s *manager) MetaMailbox() actor.MailboxSender[types.NetworkMetadataItem] {
	return s.metaInbox
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
		l.Stop()
	}
	for _, l := range s.loopsMbx {
		l.Stop()
	}

	s.metadata.Stop()
	s.metaMbx.Stop()
	s.inbox.Stop()
}

// Queue adds anything thats not metadata to the queue.
func (s *manager) Queue(ctx context.Context, hash uint64, d []byte) {
	// Based on a hash which is the label hash add to the queue.
	queueNum := hash % s.connectionCount
	s.loopsMbx[queueNum].Send(ctx, d)
}

// QueueMetadata adds metadata to the queue.
func (s *manager) QueueMetadata(ctx context.Context, d []byte) {
	s.metaMbx.Send(ctx, d)
}
