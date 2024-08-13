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
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/ortuman/nuke"
	"github.com/prometheus/prometheus/prompb"
	"github.com/vladopajic/go-actor/actor"
	"go.uber.org/atomic"
)

var _ actor.Worker = (*loop)(nil)

// loop handles the low level sending of data. It conceptually a queue.
// TODO @mattdurham think about if we need to split loop into metadata loop
type loop struct {
	configMbx      actor.Mailbox[ConnectionConfig]
	seriesMbx      actor.Mailbox[[]byte]
	metaSeriesMbx  actor.Mailbox[[]byte]
	client         *http.Client
	batchCount     int
	flushTimer     time.Duration
	cfg            ConnectionConfig
	log            log.Logger
	pbuf           *proto.Buffer
	lastSend       time.Time
	buf            []byte
	seriesBuf      []prompb.TimeSeries
	statsFunc      func(s types.NetworkStats)
	stopCh         chan struct{}
	stopCalled     atomic.Bool
	externalLabels map[string]string
	series         [][]byte
	self           actor.Actor
}

func newLoop(cc ConnectionConfig, log log.Logger, series func(s types.NetworkStats)) *loop {
	return &loop{
		seriesMbx:      actor.NewMailbox[[]byte](actor.OptCapacity(2 * cc.BatchCount)),
		configMbx:      actor.NewMailbox[ConnectionConfig](),
		metaSeriesMbx:  actor.NewMailbox[[]byte](),
		batchCount:     cc.BatchCount,
		flushTimer:     cc.FlushDuration,
		client:         &http.Client{},
		cfg:            cc,
		pbuf:           proto.NewBuffer(nil),
		buf:            make([]byte, 0),
		log:            log,
		seriesBuf:      make([]prompb.TimeSeries, 0),
		statsFunc:      series,
		externalLabels: cc.ExternalLabels,
	}
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
	timer := time.NewTimer(1 * time.Second)
	select {
	case <-ctx.Done():
		l.stopCalled.Store(true)
		return actor.WorkerEnd
	case cc := <-l.configMbx.ReceiveC():
		l.cfg = cc
		return actor.WorkerContinue
	case <-timer.C:
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
	case buf := <-l.metaSeriesMbx.ReceiveC():
		l.series = append(l.series, buf)
		if len(l.series) >= l.batchCount {
			l.trySend(l.series)
			l.series = l.series[:0]
		}
		return actor.WorkerContinue
	}
}

// trySend is the core functionality for sending data to a endpoint. It will attempt retries as defined in MaxRetryBackoffAttempts.
func (l *loop) trySend(series [][]byte) {
	attempts := 0
attempt:
	level.Debug(l.log).Log("msg", "sending data", "attempts", attempts, "len", len(series))
	start := time.Now()
	result := l.send(series, attempts)
	duration := time.Since(start)
	l.statsFunc(types.NetworkStats{
		SendDuration: duration,
	})
	level.Debug(l.log).Log("msg", "sending data result", "attempts", attempts, "successful", result.successful, "err", result.err)
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
		level.Debug(l.log).Log("msg", "max attempts reached", "attempts", attempts)
		l.finishSending()
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
}

func (l *loop) finishSending() {
	l.lastSend = time.Now()
}

func (l *loop) send(series [][]byte, retryCount int) sendResult {
	result := sendResult{}
	l.pbuf.Reset()
	l.seriesBuf = l.seriesBuf[:0]
	arena := nuke.NewMonotonicArena(256*1024, 80)
	defer arena.Reset(true)
	for _, tsBuf := range series {
		ts := nuke.New[prompb.TimeSeries](arena)
		err := ts.Unmarshal(tsBuf)
		if err != nil {
			continue
		}
		l.seriesBuf = append(l.seriesBuf, *ts)
	}
	req := &prompb.WriteRequest{
		Timeseries: l.seriesBuf,
	}
	for k, v := range l.externalLabels {
		for i, ts := range req.Timeseries {
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
			req.Timeseries[i] = ts
		}
	}
	err := l.pbuf.Marshal(req)
	if err != nil {
		result.err = err
		return result
	}

	l.buf = l.buf[:0]
	l.buf = snappy.Encode(l.buf, l.pbuf.Bytes())
	httpReq, err := http.NewRequest("POST", l.cfg.URL, bytes.NewReader(l.buf))
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
	for _, ts := range req.Timeseries {
		for _, sample := range ts.Samples {
			if sample.Timestamp > newestTS {
				newestTS = sample.Timestamp
			}
		}
	}
	l.statsFunc(types.NetworkStats{
		SeriesSent:      len(series),
		NewestTimestamp: newestTS,
	})

	result.successful = true
	return result
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
