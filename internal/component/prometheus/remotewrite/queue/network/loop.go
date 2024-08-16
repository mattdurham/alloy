package network

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
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
	seriesMbx      actor.Mailbox[*types.TimeSeries]
	metaSeriesMbx  actor.Mailbox[*types.MetaSeries]
	client         *http.Client
	batchCount     int
	flushTimer     time.Duration
	cfg            ConnectionConfig
	log            log.Logger
	lastSend       time.Time
	statsFunc      func(s types.NetworkStats)
	stopCh         chan struct{}
	stopCalled     atomic.Bool
	externalLabels map[string]string
	series         []*types.TimeSeries
	self           actor.Actor
	ticker         *time.Ticker
	req            *prompb.WriteRequest
}

func newLoop(cc ConnectionConfig, log log.Logger, series func(s types.NetworkStats)) *loop {
	l := &loop{
		seriesMbx:      actor.NewMailbox[*types.TimeSeries](actor.OptCapacity(2 * cc.BatchCount)),
		configMbx:      actor.NewMailbox[ConnectionConfig](),
		metaSeriesMbx:  actor.NewMailbox[*types.MetaSeries](),
		batchCount:     cc.BatchCount,
		flushTimer:     cc.FlushDuration,
		client:         &http.Client{},
		cfg:            cc,
		log:            log,
		statsFunc:      series,
		externalLabels: cc.ExternalLabels,
		ticker:         time.NewTicker(1 * time.Second),
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
	case cc := <-l.configMbx.ReceiveC():
		l.cfg = cc
		return actor.WorkerContinue
	default:
	}
	// Timer is to check the flush.
	select {
	case <-ctx.Done():
		l.stopCalled.Store(true)
		return actor.WorkerEnd
	case cc := <-l.configMbx.ReceiveC():
		l.cfg = cc
		return actor.WorkerContinue
	case <-l.ticker.C:
		if len(l.series) == 0 {
			return actor.WorkerContinue
		}
		if time.Since(l.lastSend) > l.flushTimer {
			l.trySend(l.series)
			l.series = l.series[:0]
		}
		return actor.WorkerContinue
	case buf := <-l.seriesMbx.ReceiveC():
		l.series = append(l.series, buf)
		if len(l.series) >= l.batchCount {
			l.trySend(l.series)
			l.series = l.series[:0]
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
func (l *loop) trySend(series []*types.TimeSeries) {
	attempts := 0
	var data []byte
attempt:
	level.Debug(l.log).Log("msg", "sending data", "attempts", attempts, "len", len(series))
	start := time.Now()
	result := l.send(series, attempts, data)
	data = result.data
	duration := time.Since(start)
	l.statsFunc(types.NetworkStats{
		SendDuration: duration,
	})
	level.Debug(l.log).Log("msg", "sending data result", "attempts", attempts, "successful", result.successful, "err", result.err)
	if result.successful {
		l.finishSending(series)
		return
	}
	if !result.recoverableError {
		l.finishSending(series)
		return
	}
	attempts++
	if attempts > int(l.cfg.MaxRetryBackoffAttempts) && l.cfg.MaxRetryBackoffAttempts > 0 {
		level.Debug(l.log).Log("msg", "max attempts reached", "attempts", attempts)
		l.finishSending(series)
		return
	}
	l.statsFunc(types.NetworkStats{
		Retries: 1,
	})
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
	data             []byte
}

func (l *loop) finishSending(series []*types.TimeSeries) {
	types.PutTimeSeriesSlice(series)
	l.lastSend = time.Now()
}

func (l *loop) send(series []*types.TimeSeries, retryCount int, data []byte) sendResult {
	result := sendResult{}
	var err error
	var buf []byte
	// Check to see if this is a retry and we can reuse the buffer.
	if len(data) == 0 {
		data, err = createWriteRequest(series, l.externalLabels, data)
		if err != nil {
			result.err = err
			return result
		}
		buf = snappy.Encode(buf, data)
	} else {
		buf = data
	}

	result.data = buf
	httpReq, err := http.NewRequest("POST", l.cfg.URL, bytes.NewReader(buf))
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
			l.statsFunc(types.NetworkStats{
				Retries429: 1,
			})
		} else {
			l.statsFunc(types.NetworkStats{
				Retries5XX: 1,
			})
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
		l.statsFunc(types.NetworkStats{
			Fails: 1,
		})
		result.err = fmt.Errorf("server returned HTTP status %s: %s", resp.Status, line)
		return result
	}

	// Find the newest
	var newestTS int64
	for _, ts := range series {
		if ts.TS > newestTS {
			newestTS = ts.TS
		}
	}
	l.statsFunc(types.NetworkStats{
		SeriesSent:      len(series),
		NewestTimestamp: newestTS,
	})

	result.successful = true
	return result
}

var tsPool = sync.Pool{New: func() any {
	return &prompb.TimeSeries{}
}}

func createWriteRequest(series []*types.TimeSeries, externalLabels map[string]string, data []byte) ([]byte, error) {
	wr := &prompb.WriteRequest{
		Timeseries: make([]prompb.TimeSeries, len(series)),
	}
	for i, tsBuf := range series {
		ts := tsPool.Get().(*prompb.TimeSeries)
		if cap(ts.Labels) < len(tsBuf.Labels) {
			ts.Labels = make([]prompb.Label, 0, len(tsBuf.Labels))
		}
		ts.Labels = ts.Labels[:len(tsBuf.Labels)]
		for k, v := range tsBuf.Labels {
			ts.Labels[k].Name = v.Name
			ts.Labels[k].Value = v.Value
		}
		if tsBuf.Histogram != nil {
			ts.Histograms = make([]prompb.Histogram, 1)
			ts.Histograms[0] = tsBuf.Histogram.ToPromHistogram()
		}
		if tsBuf.FloatHistogram != nil {
			ts.Histograms = make([]prompb.Histogram, 1)
			ts.Histograms[0] = tsBuf.FloatHistogram.ToPromFloatHistogram()
		}
		// Encode the external labels inside if needed.
		for k, v := range externalLabels {
			found := false
			for _, lbl := range ts.Labels {
				if lbl.Name == k {
					lbl.Value = v
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
		wr.Timeseries[i] = *ts
	}
	defer func() {
		for i := 0; i < len(wr.Timeseries); i++ {
			wr.Timeseries[i].Histograms = wr.Timeseries[i].Histograms[:0]
			wr.Timeseries[i].Labels = wr.Timeseries[i].Labels[:0]
			wr.Timeseries[i].Exemplars = wr.Timeseries[i].Exemplars[:0]
			tsPool.Put(&wr.Timeseries[i])
		}
	}()
	size := wr.Size()
	if cap(data) < size {
		data = make([]byte, size)
	}
	_, err := wr.MarshalTo(data)
	return data, err
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
