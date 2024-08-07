package queue

import (
	"context"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/filequeue"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"path/filepath"
	"sync"
	"time"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/cbor"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/network"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/grafana/alloy/internal/featuregate"
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

type endpoint struct {
	mut        sync.RWMutex
	fq         types.FileStorage
	client     types.NetworkClient
	serializer *cbor.Serializer
	stat       *types.PrometheusStats
	metaStats  *types.PrometheusStats
	log        log.Logger
	ctx        context.Context
	ttl        time.Duration
}

// Run starts the component, blocking until ctx is canceled or the component
// suffers a fatal error. Run is guaranteed to be called exactly once per
// Component.
func (s *Queue) Run(ctx context.Context) error {
	for _, ep := range s.args.Connections {
		fq, err := filequeue.NewQueue(filepath.Join(s.opts.DataPath, ep.Name, "wal"), s.opts.Logger)
		if err != nil {
			return err
		}
		serial, err := cbor.NewSerializer(s.args.BatchSizeBytes, s.args.FlushDuration, fq, s.opts.Logger)
		if err != nil {
			return err
		}
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
			fq:         fq,
			client:     client,
			serializer: serial,
			stat:       stats,
			metaStats:  meta,
			ctx:        ctx,
			log:        s.opts.Logger,
			ttl:        s.args.TTL,
		}
		s.endpoints[ep.Name] = end
		go end.runloop(ctx)
	}
	defer func() {
		for _, ep := range s.endpoints {
			ep.fq.Close()
		}
	}()

	<-ctx.Done()
	return nil
}

func (ep *endpoint) runloop(ctx context.Context) {
	buf := make([]byte, 0)
	var name string
	var err error
	for {
		_, buf, name, err = ep.fq.Next(ctx, buf)
		if err != nil {
			level.Error(ep.log).Log("msg", "error getting next file", "err", err)
		}

		buf, err = snappy.Decode(buf)
		if err != nil {
			level.Debug(ep.log).Log("msg", "error snappy decoding", "name", name, "err", err)
			continue
		}
		sg, err := cbor.DeserializeToSeriesGroup(buf)
		if err != nil {
			level.Debug(ep.log).Log("msg", "error deserializing", "name", name, "err", err)
			continue
		}
		func() {
			ep.mut.RLock()
			defer ep.mut.RUnlock()

			for _, series := range sg.Series {
				// One last chance to check the TTL. Writing to the filequeue will check it but
				// in a situation where the network is down and writing backs up we dont want to send
				// data that will get rejected.
				old := time.Since(time.Unix(series.TS, 0))
				if old > ep.ttl {
					continue
				}
				successful := ep.client.Queue(ctx, series.Hash, series.Bytes)
				if !successful {
					return
				}

			}
			for _, md := range sg.Metadata {
				successful := ep.client.QueueMetadata(ctx, md.Bytes)
				if !successful {
					return
				}
			}
		}()
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

var _ storage.Appender = (*fanout)(nil)

type fanout struct {
	children []storage.Appender
}

func (f fanout) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	for _, child := range f.children {
		_, err := child.Append(ref, l, t, v)
		if err != nil {
			return ref, err
		}
	}
	return ref, nil
}

func (f fanout) Commit() error {
	for _, child := range f.children {
		err := child.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (f fanout) Rollback() error {
	for _, child := range f.children {
		err := child.Rollback()
		if err != nil {
			return err
		}
	}
	return nil
}

func (f fanout) AppendExemplar(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	for _, child := range f.children {
		_, err := child.AppendExemplar(ref, l, e)
		if err != nil {
			return ref, err
		}
	}
	return ref, nil
}

func (f fanout) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	for _, child := range f.children {
		_, err := child.AppendHistogram(ref, l, t, h, fh)
		if err != nil {
			return ref, err
		}
	}
	return ref, nil
}

func (f fanout) UpdateMetadata(ref storage.SeriesRef, l labels.Labels, m metadata.Metadata) (storage.SeriesRef, error) {
	for _, child := range f.children {
		_, err := child.UpdateMetadata(ref, l, m)
		if err != nil {
			return ref, err
		}
	}
	return ref, nil
}

func (f fanout) AppendCTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64) (storage.SeriesRef, error) {
	for _, child := range f.children {
		_, err := child.AppendCTZeroSample(ref, l, t, ct)
		if err != nil {
			return ref, err
		}
	}
	return ref, nil
}
