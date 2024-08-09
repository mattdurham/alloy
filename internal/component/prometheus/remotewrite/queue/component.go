package queue

import (
	"context"
	"path/filepath"
	"sync"

	"github.com/go-kit/log"
	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/cbor"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/filequeue"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/network"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/grafana/alloy/internal/featuregate"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
)

func init() {
	component.Register(component.Registration{
		Name:      "prometheus.remote.queue",
		Args:      types.Arguments{},
		Exports:   types.Exports{},
		Stability: featuregate.StabilityExperimental,
		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return NewComponent(opts, args.(types.Arguments))
		},
	})
}

func NewComponent(opts component.Options, args types.Arguments) (*Queue, error) {
	s := &Queue{
		opts:      opts,
		args:      args,
		log:       opts.Logger,
		endpoints: map[string]*endpoint{},
	}
	s.opts.OnStateChange(types.Exports{Receiver: s})
	return s, nil
}

// Queue is a queue based WAL used to send data to a remote_write endpoint. Queue supports replaying
// and TTLs.
type Queue struct {
	mut       sync.RWMutex
	args      types.Arguments
	opts      component.Options
	log       log.Logger
	endpoints map[string]*endpoint
}

// Run starts the component, blocking until ctx is canceled or the component
// suffers a fatal error. Run is guaranteed to be called exactly once per
// Component.
func (s *Queue) Run(ctx context.Context) error {
	for _, ep := range s.args.Connections {
		reg := prometheus.WrapRegistererWith(prometheus.Labels{"endpoint": ep.Name}, s.opts.Registerer)
		stats := types.NewStats("alloy", "queue_series", reg)
		stats.BackwardsCompatibility(reg)
		meta := types.NewStats("alloy", "queue_metadata", reg)
		client, err := network.New(ctx, network.ConnectionConfig{
			URL:            ep.URL,
			Username:       ep.BasicAuth.Username,
			Password:       ep.BasicAuth.Password,
			BatchCount:     ep.BatchCount,
			FlushDuration:  ep.FlushDuration,
			Timeout:        ep.Timeout,
			UserAgent:      "alloy",
			ExternalLabels: s.args.ExternalLabels,
		}, uint64(ep.QueueCount), s.log, stats.UpdateNetwork, meta.UpdateNetwork)

		end := &endpoint{
			client:    client,
			stat:      stats,
			metaStats: meta,
			ctx:       ctx,
			log:       s.opts.Logger,
			ttl:       s.args.TTL,
		}

		fq, err := filequeue.NewQueue(filepath.Join(s.opts.DataPath, ep.Name, "wal"), func(ctx context.Context, dh types.DataHandle) {
			_ = end.mbx.Send(ctx, dh)
		}, s.opts.Logger)
		if err != nil {
			return err
		}
		serial, err := cbor.NewSerializer(s.args.BatchSizeBytes, s.args.FlushDuration, fq, s.opts.Logger)
		if err != nil {
			return err
		}
		end.serializer = serial
		s.endpoints[ep.Name] = end
		end.Start()
	}
	defer func() {
		s.mut.Lock()
		defer s.mut.Unlock()

		for _, ep := range s.endpoints {
			ep.Stop()
		}
	}()

	<-ctx.Done()
	return nil
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

	newArgs := args.(types.Arguments)
	sync.OnceFunc(func() {
		s.opts.OnStateChange(types.Exports{Receiver: s})
	})
	s.args = newArgs
	return nil
	/*
		TODO @mattdurham need to cycle through the endpoints figuring out what changed.
			if s.client == nil {
				s.args = newArgs
				return nil
			}
			if s.args.TriggerSerializationChange(newArgs) {
				s.serializer.Update(newArgs.FlushDuration, newArgs.BatchSizeBytes)
			}
			if s.args.TriggerWriteClientChange(newArgs) {
				// Send stop to all channels and rebuild.
				s.client.Stop()
				client, err := network.New(s.ctx, network.ConnectionConfig{
					URL:            s.args.Connection.URL,
					Username:       s.args.Connection.BasicAuth.Username,
					Password:       s.args.Connection.BasicAuth.Password,
					BatchCount:     s.args.Connection.BatchCount,
					FlushDuration:  s.args.Connection.FlushDuration,
					Timeout:        s.args.Connection.Timeout,
					UserAgent:      "alloy",
					ExternalLabels: s.args.ExternalLabels,
				}, uint64(s.args.Connection.QueueCount), s.log, s.stat.UpdateNetwork, s.metaStats.UpdateNetwork)
				if err != nil {
					return err
				}
				s.client = client
			}
			s.args = newArgs
			return nil

	*/
}

// Appender returns a new appender for the storage. The implementation
// can choose whether or not to use the context, for deadlines or to check
// for errors.
func (c *Queue) Appender(ctx context.Context) storage.Appender {
	c.mut.RLock()
	defer c.mut.RUnlock()

	children := make([]storage.Appender, 0)
	for _, ep := range c.endpoints {
		children = append(children, cbor.NewAppender(c.args.TTL, ep.serializer, c.args.AppenderBatchSize, ep.stat.UpdateFileQueue, c.opts.Logger))
	}
	return &fanout{children: children}
}
