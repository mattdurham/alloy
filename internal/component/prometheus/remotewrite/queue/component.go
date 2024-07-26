package queue

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/cbor"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/filequeue"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/network"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/grafana/alloy/internal/featuregate"
	"github.com/prometheus/prometheus/storage"
)

func init() {
	component.Register(component.Registration{
		Name:      "prometheus.remote.queue",
		Args:      Arguments{},
		Exports:   Exports{},
		Stability: featuregate.StabilityExperimental,
		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return NewComponent(opts, args.(Arguments))
		},
	})
}

func NewComponent(opts component.Options, args Arguments) (*Queue, error) {
	fq, err := filequeue.NewQueue(filepath.Join(opts.DataPath, "wal"), opts.Logger)
	if err != nil {
		return nil, err
	}
	serial, err := cbor.NewSerializer(args.BatchSizeBytes, args.FlushTime, fq, opts.Logger)
	if err != nil {
		return nil, err
	}
	s := &Queue{
		opts:       opts,
		args:       args,
		serializer: serial,
		fq:         fq,
		log:        opts.Logger,
	}
	s.opts.OnStateChange(Exports{Receiver: s})
	return s, nil
}

// Queue is a queue based WAL used to send data to a remote_write endpoint. Queue supports replaying
// and TTLs.
type Queue struct {
	mut        sync.RWMutex
	args       Arguments
	opts       component.Options
	serializer *cbor.Serializer
	fq         filequeue.Storage
	client     types.WriteClient
	log        log.Logger
}

// Run starts the component, blocking until ctx is canceled or the component
// suffers a fatal error. Run is guaranteed to be called exactly once per
// Component.
func (s *Queue) Run(ctx context.Context) error {
	client, err := network.New(ctx, network.ConnectionConfig{
		URL:           s.args.Connection.URL,
		Username:      s.args.Connection.BasicAuth.Username,
		Password:      s.args.Connection.BasicAuth.Password,
		BatchCount:    s.args.Connection.BatchCount,
		FlushDuration: s.args.Connection.FlushDuration,
		Timeout:       s.args.Connection.Timeout,
		UserAgent:     "alloy-dev",
	}, uint64(s.args.Connection.QueueCount), s.log)
	if err != nil {
		return err
	}
	s.client = client
	go s.runloop(ctx)
	<-ctx.Done()
	return nil
}

func (s *Queue) Update(args ConnectionConfig) {
	// This needs to drain stop the loop then apply the configuration.
	// In the case its unable to drain in a certain timeframe it should drop
	// the loops.
}

func (s *Queue) runloop(ctx context.Context) {
	buf := make([]byte, 0)
	var name string
	var err error
	for {
		_, buf, name, err = s.fq.Next(ctx, buf)
		// When we successfully grab the data then we need to delete it.
		// Even if we fail deserializing it then we should still delete it.
		s.fq.Delete(name)
		if err != nil {
			level.Error(s.log).Log("msg", "error getting next file", "err", err)
		}

		buf, err = snappy.Decode(buf)
		if err != nil {
			level.Debug(s.log).Log("msg", "error snappy decoding", "name", name, "err", err)
			continue
		}
		sg, err := cbor.DeserializeToSeriesGroup(buf)
		if err != nil {
			level.Debug(s.log).Log("msg", "error deserializing", "name", name, "err", err)
			continue
		}
		for _, series := range sg.Series {
			// One last chance to check the TTL. Writing to the filequeue will check it but
			// in a situation where the network is down and writing backs up we dont want to send
			// data that will get rejected.
			old := time.Since(time.Unix(series.TS, 0))
			if old > s.args.TTL {
				continue
			}
			// This should really return a channel that lets you know when it can queue more.
			successful := s.client.Queue(ctx, series.Hash, series.Bytes)
			if !successful {
				return
			}

		}
		for _, md := range sg.Metadata {
			successful := s.client.QueueMetadata(ctx, md.Bytes)
			if !successful {
				return
			}
		}
	}
}

// Update provides a new Config to the component. The type of newConfig will
// always match the struct type which the component registers.
//
// Update will be called concurrently with Run. The component must be able to
// gracefully handle updating its config while still running.
//
// An error may be returned if the provided config is invalid.
func (s *Queue) Update(args component.Arguments) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	//TODO @mattdurham, we need to propagate args down to the child items.
	s.args = args.(Arguments)
	s.opts.OnStateChange(Exports{Receiver: s})

	return nil
}

// Appender returns a new appender for the storage. The implementation
// can choose whether or not to use the context, for deadlines or to check
// for errors.
func (c *Queue) Appender(ctx context.Context) storage.Appender {
	c.mut.RLock()
	defer c.mut.RUnlock()

	return cbor.NewAppender(c.args.TTL, c.serializer, c.args.AppenderBatchSize, c.opts.Logger)
}
