package filequeue

import (
	"context"
	"fmt"
	"github.com/grafana/alloy/internal/runtime/logging/level"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/fxamacker/cbor/v2"
	"github.com/go-kit/log"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/vladopajic/go-actor/actor"
)

var _ actor.Worker = (*queue)(nil)

type queue struct {
	self      actor.Actor
	directory string
	maxIndex  int
	logger    log.Logger
	inbox     actor.Mailbox[types.Data]
	out       func(ctx context.Context, dh types.DataHandle)
	ids       []string
}

func (q *queue) Start() {
	q.self = actor.Combine(actor.New(q), q.inbox).Build()
	q.self.Start()
	// Queue up our existing items.
	for _, name := range q.ids {
		q.out(context.TODO(), types.DataHandle{
			Name: name,
			Get: func() (map[string]string, []byte, error) {
				return get(name)
			},
		})
	}
}

// Record wraps the input data and combines it with the metadata.
type Record struct {
	Meta map[string]string
	Data []byte
}

// NewQueue returns a implementation of FileStorage.
func NewQueue(directory string, out func(ctx context.Context, dh types.DataHandle), logger log.Logger) (types.FileStorage, error) {
	err := os.MkdirAll(directory, 0777)
	if err != nil {
		return nil, err
	}

	matches, _ := filepath.Glob(filepath.Join(directory, "*.committed"))
	ids := make([]int, len(matches))

	for i, x := range matches {
		id, err := strconv.Atoi(strings.ReplaceAll(filepath.Base(x), ".committed", ""))
		if err != nil {
			continue
		}
		ids[i] = id
	}
	sort.Ints(ids)
	var currentIndex int
	if len(ids) > 0 {
		currentIndex = ids[len(ids)-1]
	}
	q := &queue{
		directory: directory,
		maxIndex:  currentIndex,
		logger:    logger,
		out:       out,
		inbox:     actor.NewMailbox[types.Data](),
		ids:       make([]string, 0),
	}

	// Push the files that currently exist to the channel.
	for _, id := range ids {
		name := filepath.Join(directory, fmt.Sprintf("%d.committed", id))
		q.ids = append(q.ids, name)
	}
	return q, nil
}

func (q *queue) Send(ctx context.Context, meta map[string]string, data []byte) error {
	return q.inbox.Send(ctx, types.Data{
		Meta: meta,
		Data: data,
	})
}

func (q *queue) Stop() {
	q.self.Stop()
}

func get(name string) (map[string]string, []byte, error) {
	buf, err := readFile(name)
	defer deleteFile(name)
	if err != nil {
		return nil, nil, err
	}
	r := &Record{}
	err = cbor.Unmarshal(buf, r)
	if err != nil {
		return nil, nil, err
	}
	return r.Meta, r.Data, nil
}

func (q *queue) DoWork(ctx actor.Context) actor.WorkerStatus {
	select {
	case <-ctx.Done():
		return actor.WorkerEnd
	case item := <-q.inbox.ReceiveC():
		level.Debug(q.logger).Log("msg", "received item")
		name, err := q.add(item.Meta, item.Data)
		if err != nil {
			level.Error(q.logger).Log("msg", "error adding item", "err", err)
			return actor.WorkerContinue
		}
		level.Debug(q.logger).Log("msg", "writing item")
		q.out(ctx, types.DataHandle{
			Name: name,
			Get: func() (map[string]string, []byte, error) {
				return get(name)
			},
		})
		return actor.WorkerContinue
	}
}

// Add a committed file to the queue.
func (q *queue) add(meta map[string]string, data []byte) (string, error) {
	if meta == nil {
		meta = make(map[string]string)
	}
	q.maxIndex++
	name := filepath.Join(q.directory, fmt.Sprintf("%d.committed", q.maxIndex))
	// record wraps the data and metadata in one. This allows the consumer to take action based on the map.
	r := &Record{
		Meta: meta,
		Data: data,
	}
	rBuf, err := cbor.Marshal(r)
	if err != nil {
		return "", err
	}
	err = q.writeFile(name, rBuf)
	if err != nil {
		return "", err
	}
	return name, err
}

func deleteFile(name string) {
	_ = os.Remove(name)
}

func (q *queue) writeFile(name string, data []byte) error {
	return os.WriteFile(name, data, 0644)
}

func readFile(name string) ([]byte, error) {
	bb, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}
	return bb, err
}
