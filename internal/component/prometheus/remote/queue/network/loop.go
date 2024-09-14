package network

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/alloy/internal/component/prometheus/remote/queue/types"
	"github.com/prometheus/prometheus/prompb"
	"github.com/vladopajic/go-actor/actor"
	"go.uber.org/atomic"
)

var _ actor.Worker = (*loop)(nil)

// loop handles the low level sending of data. It conceptually a queue.
// loop makes no attempt to save or restore signals in the queue.
// loop config cannot be updated, it is easier to recreate. This does mean we lose any signals in the queue.
type loop struct {
	isMeta         bool
	seriesMbx      actor.Mailbox[*types.TimeSeriesBinary]
	client         *http.Client
	batchCount     int
	flushFrequency time.Duration
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

func newLoop(cc ConnectionConfig, isMetaData bool, log log.Logger, stats func(s types.NetworkStats)) *loop {
	l := &loop{
		isMeta: isMetaData,
		// In general we want a healthy queue of items, in this case we want to have 2x our maximum send sized ready.
		seriesMbx:      actor.NewMailbox[*types.TimeSeriesBinary](actor.OptCapacity(2 * cc.BatchCount)),
		batchCount:     cc.BatchCount,
		flushFrequency: cc.FlushFrequency,
		client:         &http.Client{},
		cfg:            cc,
		log:            log,
		statsFunc:      stats,
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
		l.seriesMbx,
	}
}

func (l *loop) DoWork(ctx actor.Context) actor.WorkerStatus {
	// Main select loop
	select {
	case <-ctx.Done():
		l.stopCalled.Store(true)
		return actor.WorkerEnd
	// Ticker is to ensure the flush timer is called.
	case <-l.ticker.C:
		if len(l.series) == 0 {
			return actor.WorkerContinue
		}
		if time.Since(l.lastSend) > l.flushFrequency {
			l.trySend(ctx)
		}
		return actor.WorkerContinue
	case series, ok := <-l.seriesMbx.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		l.series = append(l.series, series)
		if len(l.series) >= l.batchCount {
			l.trySend(ctx)
		}
		return actor.WorkerContinue
	}
}

// trySend is the core functionality for sending data to a endpoint. It will attempt retries as defined in MaxRetryBackoffAttempts.
func (l *loop) trySend(ctx context.Context) {
	attempts := 0
attempt:
	start := time.Now()
	result := l.send(ctx, attempts)
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
	// This helps us short circuit the loop if we are stopping.
	if l.stopCalled.Load() {
		return
	}
	// Sleep between attempts.
	time.Sleep(result.retryAfter)
	goto attempt
}

type sendResult struct {
	err              error
	successful       bool
	recoverableError bool
	retryAfter       time.Duration
	statusCode       int
	networkError     bool
}

func (l *loop) finishSending() {
	types.PutTimeSeriesBinarySlice(l.series)
	l.sendBuffer = l.sendBuffer[:0]
	l.series = make([]*types.TimeSeriesBinary, 0, l.batchCount)
	l.lastSend = time.Now()
}

// send is the main work loop of the loop.
func (l *loop) send(ctx context.Context, retryCount int) sendResult {
	result := sendResult{}
	defer func() {
		recordStats(l.series, l.isMeta, l.statsFunc, result, len(l.sendBuffer))
	}()
	var err error
	// Check to see if this is a retry and we can reuse the buffer.
	if len(l.sendBuffer) == 0 {
		var data []byte
		var wrErr error
		if l.isMeta {
			data, wrErr = createWriteRequestMetadata(l.req, l.series, l.buf)
		} else {
			data, wrErr = createWriteRequest(l.req, l.series, l.externalLabels, l.buf)
		}
		if wrErr != nil {
			result.err = wrErr
			result.recoverableError = false
			return result
		}
		l.sendBuffer = snappy.Encode(l.sendBuffer, data)
	}

	httpReq, err := http.NewRequest("POST", l.cfg.URL, bytes.NewReader(l.sendBuffer))
	if err != nil {
		result.err = err
		result.recoverableError = true
		result.networkError = true
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
	ctx, cncl := context.WithTimeout(ctx, l.cfg.Timeout)
	defer cncl()
	resp, err := l.client.Do(httpReq.WithContext(ctx))
	// Network errors are recoverable.
	if err != nil {
		result.err = err
		result.networkError = true
		result.recoverableError = true
		result.retryAfter = l.cfg.RetryBackoff
		return result
	}
	result.statusCode = resp.StatusCode
	defer resp.Body.Close()
	// 500 errors are considered recoverable.
	if resp.StatusCode/100 == 5 || resp.StatusCode == http.StatusTooManyRequests {
		result.retryAfter = retryAfterDuration(l.cfg.RetryBackoff, resp.Header.Get("Retry-After"))
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
		result.err = fmt.Errorf("server returned HTTP status %s: %s", resp.Status, line)
		return result
	}

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

		// By default each sample only has a histogram, float histogram or sample.
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
		// By default each TimeSeries only has one sample.
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
	// Reset the buffer for reuse.
	data.Reset()
	err := data.Marshal(wr)
	return data.Bytes(), err
}

func createWriteRequestMetadata(wr *prompb.WriteRequest, series []*types.TimeSeriesBinary, data *proto.Buffer) ([]byte, error) {
	if cap(wr.Metadata) < len(series) {
		wr.Metadata = make([]prompb.MetricMetadata, len(series))
	} else {
		wr.Metadata = wr.Metadata[:len(series)]
	}

	for i, ts := range series {
		mt, valid := toMetadata(ts)
		if !valid {
			continue
		}
		wr.Metadata[i] = mt
	}
	data.Reset()
	err := data.Marshal(wr)
	return data.Bytes(), err
}

func getMetadataCount(tss []*types.TimeSeriesBinary) int {
	var cnt int
	for _, ts := range tss {
		if isMetadata(ts) {
			cnt++
		}
	}
	return cnt
}

func isMetadata(ts *types.TimeSeriesBinary) bool {
	return ts.Labels.Has("__alloy_metadata_type__") &&
		ts.Labels.Has("__alloy_metadata_unit__") &&
		ts.Labels.Has("__alloy_metadata_help__")
}

func toMetadata(ts *types.TimeSeriesBinary) (prompb.MetricMetadata, bool) {
	if !isMetadata(ts) {
		return prompb.MetricMetadata{}, false
	}
	return prompb.MetricMetadata{
		Type:             prompb.MetricMetadata_MetricType(prompb.MetricMetadata_MetricType_value[strings.ToUpper(ts.Labels.Get("__alloy_metadata_type__"))]),
		Help:             ts.Labels.Get("__alloy_metadata_help__"),
		Unit:             ts.Labels.Get("__alloy_metadata_unit__"),
		MetricFamilyName: ts.Labels.Get("__name__"),
	}, true
}

func retryAfterDuration(defaultDuration time.Duration, t string) time.Duration {
	parsedTime, err := time.Parse(http.TimeFormat, t)
	if err == nil {
		return time.Until(parsedTime)
	}
	// The duration can be in seconds.
	d, err := strconv.Atoi(t)
	if err != nil {
		return defaultDuration
	}
	return time.Duration(d) * time.Second
}
