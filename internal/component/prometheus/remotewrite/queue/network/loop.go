package network

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/prometheus/prometheus/prompb"
	"github.com/vladopajic/go-actor/actor"
	"go.uber.org/atomic"
)

var _ actor.Worker = (*loop)(nil)

// loop handles the low level sending of data. It conceptually a queue.
// TODO @mattdurham think about if we need to split loop into metadata loop
type loop struct {
	configMbx      actor.Mailbox[ConnectionConfig]
	seriesMbx      actor.Mailbox[*types.TimeSeriesBinary]
	metaSeriesMbx  actor.Mailbox[*types.MetaSeriesBinary]
	client         *http.Client
	batchCount     int
	flushTimer     time.Duration
	cfg            ConnectionConfig
	log            log.Logger
	lastSend       time.Time
	statsFunc      func(s types.NetworkStats)
	stopCalled     atomic.Bool
	externalLabels map[string]string
	series         []*types.TimeSeriesBinary
	self           actor.Actor
	ticker         *time.Ticker
	req            *prompb.WriteRequest
	buf            *proto.Buffer
	sendBuffer     []byte
}

func newLoop(cc ConnectionConfig, log log.Logger, series func(s types.NetworkStats)) *loop {
	l := &loop{
		seriesMbx:      actor.NewMailbox[*types.TimeSeriesBinary](actor.OptCapacity(2 * cc.BatchCount)),
		configMbx:      actor.NewMailbox[ConnectionConfig](),
		metaSeriesMbx:  actor.NewMailbox[*types.MetaSeriesBinary](),
		batchCount:     cc.BatchCount,
		flushTimer:     cc.FlushDuration,
		client:         &http.Client{},
		cfg:            cc,
		log:            log,
		statsFunc:      series,
		externalLabels: cc.ExternalLabels,
		ticker:         time.NewTicker(1 * time.Second),
		buf:            proto.NewBuffer(nil),
		sendBuffer:     make([]byte, 0),
	}
	l.req = &prompb.WriteRequest{
		// We know BatchCount is the most we will ever send.
		Timeseries: make([]prompb.TimeSeries, 0, cc.BatchCount),
	}

	return l
}

func (l *loop) Start() {
	l.self = actor.Combine(l.actors()...).Build()
	l.self.Start()
}

func (l *loop) Stop() {
	l.stopCalled.Store(true)
	l.self.Stop()
}

func (l *loop) actors() []actor.Actor {
	return []actor.Actor{
		actor.New(l),
		l.metaSeriesMbx,
		l.seriesMbx,
		l.configMbx,
	}
}

func (l *loop) DoWork(ctx actor.Context) actor.WorkerStatus {
	// This first select is to prioritize configuration changes.
	select {
	case <-ctx.Done():
		l.stopCalled.Store(true)
		return actor.WorkerEnd
	case cc, ok := <-l.configMbx.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		l.cfg = cc
		return actor.WorkerContinue
	default:
	}
	// Main select loop
	select {
	case <-ctx.Done():
		l.stopCalled.Store(true)
		return actor.WorkerEnd
	case cc, ok := <-l.configMbx.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		l.cfg = cc
		return actor.WorkerContinue
	// Ticker is to ensure the flush timer is called.
	case <-l.ticker.C:
		if len(l.series) == 0 {
			return actor.WorkerContinue
		}
		if time.Since(l.lastSend) > l.flushTimer {
			l.trySend()
		}
		return actor.WorkerContinue
	case series, ok := <-l.seriesMbx.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		l.series = append(l.series, series)
		if len(l.series) >= l.batchCount {
			l.trySend()
		}
		return actor.WorkerContinue
	case <-l.metaSeriesMbx.ReceiveC():
		/*l.series = append(l.series, buf)
		if len(l.series) >= l.batchCount {
			l.trySend(l.series)
			l.series = l.series[:0]
		}*/
		return actor.WorkerContinue
	}
}

// trySend is the core functionality for sending data to a endpoint. It will attempt retries as defined in MaxRetryBackoffAttempts.
func (l *loop) trySend() {
	attempts := 0
attempt:
	start := time.Now()
	result := l.send(attempts)
	duration := time.Since(start)
	l.statsFunc(types.NetworkStats{
		SendDuration: duration,
	})
	if result.successful {
		l.finishSending()
		return
	}
	if !result.recoverableError {
		l.finishSending()
		return
	}
	attempts++
	if attempts > int(l.cfg.MaxRetryBackoffAttempts) && l.cfg.MaxRetryBackoffAttempts > 0 {
		level.Debug(l.log).Log("msg", "max retry attempts reached", "attempts", attempts)
		l.finishSending()
		return
	}
	if l.stopCalled.Load() {
		return
	}
	goto attempt
}

type sendResult struct {
	err              error
	successful       bool
	recoverableError bool
	retryAfter       time.Duration
}

func (l *loop) finishSending() {
	types.PutTimeSeriesBinarySlice(l.series)
	l.sendBuffer = l.sendBuffer[:0]
	l.series = make([]*types.TimeSeriesBinary, 0, l.batchCount)
	l.lastSend = time.Now()
}

func (l *loop) send(retryCount int) sendResult {
	// TODO @mattdurham fill in all the various stat functions.
	result := sendResult{}
	var err error
	// Check to see if this is a retry and we can reuse the buffer.
	if len(l.sendBuffer) == 0 {
		data, err := createWriteRequest(l.req, l.series, l.externalLabels, l.buf)
		if err != nil {
			result.err = err
			return result
		}
		l.sendBuffer = snappy.Encode(l.sendBuffer, data)
	}

	httpReq, err := http.NewRequest("POST", l.cfg.URL, bytes.NewReader(l.sendBuffer))
	if err != nil {
		result.err = err
		return result
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", l.cfg.UserAgent)
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	httpReq.SetBasicAuth(l.cfg.Username, l.cfg.Password)

	if retryCount > 0 {
		httpReq.Header.Set("Retry-Attempt", strconv.Itoa(retryCount))
	}
	ctx := context.Background()
	ctx, cncl := context.WithTimeout(ctx, l.cfg.Timeout)
	defer cncl()
	resp, err := l.client.Do(httpReq.WithContext(ctx))
	// Network errors are recoverable.
	if err != nil {
		result.err = err
		result.recoverableError = true
		return result
	}
	defer resp.Body.Close()
	// 500 errors are considered recoverable.
	if resp.StatusCode/100 == 5 || resp.StatusCode == http.StatusTooManyRequests {
		if resp.StatusCode == http.StatusTooManyRequests {
			l.statsFunc(types.NetworkStats{})
		} else {
			l.statsFunc(types.NetworkStats{})
		}
		result.retryAfter = retryAfterDuration(resp.Header.Get("Retry-After"))
		result.recoverableError = true
		return result
	}
	// Status Codes that are not 500 or 200 are not recoverable and dropped.
	if resp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(resp.Body, 1_000))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		l.statsFunc(types.NetworkStats{})
		result.err = fmt.Errorf("server returned HTTP status %s: %s", resp.Status, line)
		return result
	}

	// Find the newest
	var newestTS int64
	for _, ts := range l.series {
		if ts.TS > newestTS {
			newestTS = ts.TS
		}
	}
	l.statsFunc(types.NetworkStats{
		NewestTimestamp: newestTS,
	})

	result.successful = true
	return result
}

func createWriteRequest(wr *prompb.WriteRequest, series []*types.TimeSeriesBinary, externalLabels map[string]string, data *proto.Buffer) ([]byte, error) {
	if cap(wr.Timeseries) < len(series) {
		wr.Timeseries = make([]prompb.TimeSeries, len(series))
	}
	wr.Timeseries = wr.Timeseries[:len(series)]
	for i, tsBuf := range series {
		ts := wr.Timeseries[i]
		if cap(ts.Labels) < len(tsBuf.Labels) {
			ts.Labels = make([]prompb.Label, 0, len(tsBuf.Labels))
		}
		ts.Labels = ts.Labels[:len(tsBuf.Labels)]
		for k, v := range tsBuf.Labels {
			ts.Labels[k].Name = v.Name
			ts.Labels[k].Value = v.Value
		}
		if cap(ts.Histograms) == 0 {
			ts.Histograms = make([]prompb.Histogram, 1)
		} else {
			ts.Histograms = ts.Histograms[:0]
		}
		if tsBuf.Histograms.Histogram != nil {
			ts.Histograms = ts.Histograms[:1]
			ts.Histograms[0] = tsBuf.Histograms.Histogram.ToPromHistogram()
		}
		if tsBuf.Histograms.FloatHistogram != nil {
			ts.Histograms = ts.Histograms[:1]
			ts.Histograms[0] = tsBuf.Histograms.FloatHistogram.ToPromFloatHistogram()
		}

		if tsBuf.Histograms.Histogram == nil && tsBuf.Histograms.FloatHistogram == nil {
			ts.Histograms = ts.Histograms[:0]
		}

		// Encode the external labels inside if needed.
		for k, v := range externalLabels {
			found := false
			for j, lbl := range ts.Labels {
				if lbl.Name == k {
					ts.Labels[j].Value = v
					found = true
					break
				}
			}
			if !found {
				ts.Labels = append(ts.Labels, prompb.Label{
					Name:  k,
					Value: v,
				})
			}
		}
		if len(ts.Samples) == 0 {
			ts.Samples = make([]prompb.Sample, 1)
		}
		ts.Samples[0].Value = tsBuf.Value
		ts.Samples[0].Timestamp = tsBuf.TS
		wr.Timeseries[i] = ts
	}
	defer func() {
		for i := 0; i < len(wr.Timeseries); i++ {
			wr.Timeseries[i].Histograms = wr.Timeseries[i].Histograms[:0]
			wr.Timeseries[i].Labels = wr.Timeseries[i].Labels[:0]
			wr.Timeseries[i].Exemplars = wr.Timeseries[i].Exemplars[:0]
		}
	}()
	data.Reset()
	err := data.Marshal(wr)
	return data.Bytes(), err
}

func retryAfterDuration(t string) time.Duration {
	parsedDuration, err := time.Parse(http.TimeFormat, t)
	if err == nil {
		return parsedDuration.Sub(time.Now().UTC())
	}
	// The duration can be in seconds.
	d, err := strconv.Atoi(t)
	if err != nil {
		return 5
	}
	return time.Duration(d) * time.Second
}
