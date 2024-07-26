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
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/alloy/internal/alloy/logging/level"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/atomic"
	"golang.design/x/chann"
)

// loop handles the low level sending of data. It conceptually a queue.
type loop struct {
	client     *http.Client
	batchCount int
	flushTimer time.Duration
	cfg        ConnectionConfig
	log        log.Logger
	pbuf       *proto.Buffer
	lastSend   time.Time
	buf        []byte
	ch         *chann.Chann[*types.Item]
	seriesBuf  []prompb.TimeSeries
	statsFunc  func(s Stats)
	stopCh     chan struct{}
	stopCalled atomic.Bool
}

type Stats struct {
	SeriesSent   int
	Fails        int
	Retries      int
	Retry429     int
	Retry5XX     int
	SendDuration time.Duration
}

func (l *loop) runLoop(ctx context.Context) {
	series := make([]*types.Item, 0)
	for {
		// This mainly exists so a very low flush time does not steal the select from reading from the out channel.
		checkTime := time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			return
		case <-checkTime.C:
			if len(series) == 0 {
				continue
			}
			if time.Since(l.lastSend) > l.flushTimer {
				l.trySend(series)
				series = series[:0]
			}
		case buf := <-l.ch.Out():
			series = append(series, buf)
			if len(series) >= l.batchCount {
				l.trySend(series)
				series = series[:0]
			}
		case <-l.stopCh:
			return
		}
	}
}

// Push will push to the channel, it will block until it is able to or the context finishes.
func (l *loop) Push(ctx context.Context, d *types.Item) bool {
	select {
	case l.ch.In() <- d:
		return true
	case <-ctx.Done():
		return false
	case <-l.stopCh:
		return false
	}
}

func (l *loop) drain(ctx context.Context) []*types.Item {
	items := make([]*types.Item, 0)
	for {
		select {
		case item := <-l.ch.Out():
			items = append(items, item)
		case <-ctx.Done():
			return items
		}
	}
}

// trySend is the core functionality for sending data to a endpoint. It will attempt retries as defined in MaxRetryBackoffAttempts.
func (l *loop) trySend(series []*types.Item) {
	attempts := 0
attempt:
	level.Debug(l.log).Log("msg", "sending data", "attempts", attempts, "len", len(series))
	start := time.Now()
	result := l.send(series, attempts)
	duration := time.Since(start)
	l.statsFunc(Stats{
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
	l.statsFunc(Stats{
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

func (l *loop) send(series []*types.Item, retryCount int) sendResult {
	result := sendResult{}
	l.pbuf.Reset()
	l.seriesBuf = l.seriesBuf[:0]
	for _, tsBuf := range series {
		ts := prompb.TimeSeries{}
		err := proto.Unmarshal(tsBuf.Bytes, &ts)
		if err != nil {
			continue
		}
		l.seriesBuf = append(l.seriesBuf, ts)
	}
	req := &prompb.WriteRequest{
		Timeseries: l.seriesBuf,
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
			l.statsFunc(Stats{
				Retry429: 1,
			})
		} else {
			l.statsFunc(Stats{
				Retry5XX: 1,
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
		l.statsFunc(Stats{
			Fails: 1,
		})
		result.err = fmt.Errorf("server returned HTTP status %s: %s", resp.Status, line)
		return result
	}
	l.statsFunc(Stats{
		SeriesSent: len(series),
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
