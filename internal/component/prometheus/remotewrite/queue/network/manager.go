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
	"golang.design/x/chann"
)

type manager struct {
	mut             sync.Mutex
	connectionCount uint64
	loops           []*loop
	metadata        *loop
	logger          log.Logger
}

var _ types.NetworkClient = (*manager)(nil)

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
}

func New(ctx context.Context, cc ConnectionConfig, connectionCount uint64, logger log.Logger, seriesStats, metadataStats func(types.Stats)) (types.NetworkClient, error) {
	s := &manager{
		connectionCount: connectionCount,
		loops:           make([]*loop, 0),
		logger:          logger,
	}

	// start kicks off a number of concurrent connections.
	var i uint64
	for ; i < s.connectionCount; i++ {
		l := &loop{
			batchCount: cc.BatchCount,
			flushTimer: cc.FlushDuration,
			client:     &http.Client{},
			cfg:        cc,
			pbuf:       proto.NewBuffer(nil),
			buf:        make([]byte, 0),
			log:        logger,
			// We create this with double the batch so that we can always be feeding the queue.
			ch:        chann.New[[]byte](chann.Cap(cc.BatchCount * 2)),
			seriesBuf: make([]prompb.TimeSeries, 0),
			statsFunc: seriesStats,
		}
		s.loops = append(s.loops, l)
		go l.runLoop(ctx)
	}
	s.metadata = &loop{
		batchCount: cc.BatchCount,
		flushTimer: cc.FlushDuration,
		client:     &http.Client{},
		cfg:        cc,
		pbuf:       proto.NewBuffer(nil),
		buf:        make([]byte, 0),
		log:        logger,
		// We create this with double the batch so that we can always be feeding the queue.
		ch:        chann.New[[]byte](chann.Cap(cc.BatchCount * 2)),
		seriesBuf: make([]prompb.TimeSeries, 0),
		statsFunc: metadataStats,
	}
	return s, nil
}

func (s *manager) Stop() {
	for _, l := range s.loops {
		l.stopCh <- struct{}{}
	}
}

// Queue adds anything thats not metadata to the queue.
func (s *manager) Queue(ctx context.Context, hash uint64, d []byte) bool {
	// Based on a hash which is the label hash add to the queue.
	queueNum := hash % s.connectionCount

	return s.loops[queueNum].Push(ctx, d)
}

// QueueMetadata adds metadata to the queue.
func (s *manager) QueueMetadata(ctx context.Context, d []byte) bool {
	return s.metadata.Push(ctx, d)
}
