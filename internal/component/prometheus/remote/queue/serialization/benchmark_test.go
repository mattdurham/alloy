package serialization

import (
	"context"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	log2 "github.com/go-kit/log"
	"github.com/grafana/alloy/internal/component/prometheus/remote/queue/types"
	"github.com/grafana/alloy/internal/static/metrics/wal"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

type test struct {
	name        string
	metricCount int
}

var tests = []test{
	/*{
		name:        "1_000",
		metricCount: 1_000,
	},
	{
		name:        "10_000",
		metricCount: 10_000,
	},
	{
		name:        "100_000",
		metricCount: 100_000,
	},
	{
		name:        "500_000",
		metricCount: 500_000,
	},*/
	{
		name:        "1_000_000",
		metricCount: 1_000_000,
	},
}

func BenchmarkSimple(b *testing.B) {
	for _, t := range tests {
		b.Run(t.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				b.ReportAllocs()
				totalBytes := 0
				totalMemory := 0
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)
				q := &fq{}
				l := log2.NewNopLogger()
				wr, err := NewSerializer(types.SerializerConfig{
					MaxSignalsInBatch: 100_000,
					FlushFrequency:    500 * time.Millisecond,
				}, q, l)
				wr.Start()
				b.Cleanup(func() {
					wr.Stop()
				})
				require.NoError(b, err)
				app := NewAppender(context.Background(), 1*time.Minute, wr, 100, func(s types.FileQueueStats) {
				}, l)

				tSeries := &ts{
					v: 0,
					t: 0,
					l: labels.EmptyLabels(),
				}
				g := generate{
					metricCount: t.metricCount,
					labelCount:  10,
					current:     0,
				}
				var keep bool
				for {
					tSeries, keep = g.nextSeries(b, tSeries)
					if !keep {
						break
					}
					_, err = app.Append(0, tSeries.l, tSeries.t, tSeries.v)
					require.NoError(b, err)
				}
				_ = app.Commit()
				totalBytes += q.totalBytes
				runtime.ReadMemStats(&m2)
				totalMemory = int(m2.HeapInuse - m1.HeapInuse)

				b.Log("bytes written", humanize.Bytes(uint64(totalBytes)))
				b.Log("memory used per run", humanize.Bytes(uint64(totalMemory)))
			}

		})
	}

}

func BenchmarkTSDB(b *testing.B) {
	for _, t := range tests {
		b.Run(t.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				b.ReportAllocs()
				totalBytes := 0
				totalMemory := 0
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)
				l := log2.NewNopLogger()
				dir := b.TempDir()
				store, err := wal.NewStorage(l, prometheus.NewRegistry(), dir)
				require.NoError(b, err)
				app := store.Appender(context.Background())
				tSeries := &ts{
					v: 0,
					t: 0,
					l: labels.EmptyLabels(),
				}
				g := generate{
					metricCount: t.metricCount,
					labelCount:  10,
					current:     0,
				}
				var keep bool
				for {
					tSeries, keep = g.nextSeries(b, tSeries)
					if !keep {
						break
					}
					_, err = app.Append(0, tSeries.l, tSeries.t, tSeries.v)
					require.NoError(b, err)
				}
				err = app.Commit()
				require.NoError(b, err)
				filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
					if d.IsDir() {
						return nil
					}
					f, serr := os.Stat(path)
					require.NoError(b, serr)
					totalBytes += int(f.Size())
					return nil
				})
				runtime.ReadMemStats(&m2)
				totalMemory = int(m2.HeapInuse - m1.HeapInuse)

				b.Log("bytes written", humanize.Bytes(uint64(totalBytes)))
				b.Log("memory used per run", humanize.Bytes(uint64(totalMemory)))
			}
		})
	}
}

type generate struct {
	metricCount int
	labelCount  int
	current     int
}

func (g *generate) nextSeries(b *testing.B, tSeries *ts) (*ts, bool) {
	b.StopTimer()
	if g.current == g.metricCount {
		return nil, false
	}
	tSeries.l = tSeries.l[:0]
	tSeries.v = rand.Float64()
	tSeries.t = time.Now().UTC().Unix()
	tSeries.l = tSeries.l[:0]

	tSeries.l = append(tSeries.l, labels.Label{
		Name:  "__name__",
		Value: strconv.Itoa(g.current),
	})
	for j := 0; j < g.labelCount; j++ {
		tSeries.l = append(tSeries.l, labels.Label{
			Name:  fmt.Sprintf("name_%d", j),
			Value: fmt.Sprintf("value_%d", rand.Intn(20)),
		})
	}
	g.current++
	b.StartTimer()
	return tSeries, true
}

type ts struct {
	v float64
	t int64
	l labels.Labels
}

type fq struct {
	totalBytes int
}

func (f *fq) Start() {

}

func (f *fq) Stop() {

}

func (f *fq) Send(ctx context.Context, meta map[string]string, value []byte) error {
	f.totalBytes = f.totalBytes + len(value)
	return nil
}

func (f fq) Delete(_ string) {
}

func (f fq) Close() {}
