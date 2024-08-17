package queue

import (
	"context"
	"time"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/vladopajic/go-actor/actor"
)

var _ actor.Worker = (*endpoint)(nil)

// endpoint handles communication between the serializer, filequeue and network.
type endpoint struct {
	network    types.NetworkClient
	serializer types.Serializer
	stat       *types.PrometheusStats
	metaStats  *types.PrometheusStats
	log        log.Logger
	ctx        context.Context
	ttl        time.Duration
	incoming   actor.Mailbox[types.DataHandle]
	buf        []byte
	self       actor.Actor
	sg         *types.SeriesGroup
}

func NewEndpoint(client types.NetworkClient, serializer types.Serializer, stats, metatStats *types.PrometheusStats, ttl time.Duration, logger log.Logger) *endpoint {
	return &endpoint{
		network:    client,
		serializer: serializer,
		stat:       stats,
		metaStats:  metatStats,
		log:        logger,
		ttl:        ttl,
		incoming:   actor.NewMailbox[types.DataHandle](),
		buf:        make([]byte, 0, 1024),
		sg:         &types.SeriesGroup{},
	}
}

func (ep *endpoint) Start() {
	ep.self = actor.Combine(actor.New(ep), ep.incoming).Build()
	ep.self.Start()
	ep.serializer.Start()
	ep.network.Start()
}

func (ep *endpoint) Stop() {
	ep.serializer.Stop()
	ep.network.Stop()
	ep.network.Stop()
	ep.self.Stop()
}

func (ep *endpoint) DoWork(ctx actor.Context) actor.WorkerStatus {
	select {
	case <-ctx.Done():
		return actor.WorkerEnd
	case file, ok := <-ep.incoming.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		meta, buf, err := file.Get()
		if err != nil {
			return actor.WorkerEnd
		}
		ep.deserializeAndSend(ctx, meta, buf)
		return actor.WorkerContinue
	}
}

func (ep *endpoint) deserializeAndSend(ctx context.Context, _ map[string]string, buf []byte) {
	var err error
	ep.buf, err = snappy.Decode(buf)
	if err != nil {
		level.Debug(ep.log).Log("msg", "error snappy decoding", "err", err)
		return
	}
	var sg *types.SeriesGroup
	sg, ep.buf, err = types.DeserializeToSeriesGroup(ep.sg, ep.buf)
	if err != nil {
		level.Debug(ep.log).Log("msg", "error deserializing", "err", err)
		return
	}

	for _, series := range sg.Series {
		// One last chance to check the TTL. Writing to the filequeue will check it but
		// in a situation where the network is down and writing backs up we dont want to send
		// data that will get rejected.
		seriesAge := time.Since(time.Unix(series.TS, 0))
		if seriesAge > ep.ttl {
			continue
		}
		sendErr := ep.network.SendSeries(ctx, series.Hash, types.BinaryToTimeSeries(series, sg.Strings))
		if sendErr != nil {
			level.Error(ep.log).Log("msg", "error sending to write client", "err", sendErr)
		}
	}
	/*
		for _, md := range sg.Metadata {
			sendErr := ep.network.SendMetadata(context.Background(), md)
			if sendErr != nil {
				level.Error(ep.log).Log("msg", "error sending metadata to write client", "err", sendErr)
			}
		}*/
}
