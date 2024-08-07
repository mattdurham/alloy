package filequeue

import (
	"context"
	"errors"
	"fmt"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/go-kit/log"
	"golang.design/x/chann"
)

type queue struct {
	mut       sync.RWMutex
	directory string
	maxIndex  int
	logger    log.Logger
	// ch is an unbounded queue. It contains the names of files in the WAL.
	ch *chann.Chann[string]
}

// Record wraps the input data and combines it with the metadata.
type Record struct {
	Meta map[string]string `cbor:"1,keyasint"`
	Data []byte            `cbor:"2,keyasint"`
}

// NewQueue returns a implementation of FileStorage.
func NewQueue(directory string, logger log.Logger) (types.FileStorage, error) {
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
		ch:        chann.New[string](),
	}
	// Push the files that currently exist to the channel.
	for _, id := range ids {
		q.ch.In() <- filepath.Join(directory, fmt.Sprintf("%d.committed", id))
	}
	return q, nil
}

// Add a committed file to the queue.
func (q *queue) Add(meta map[string]string, data []byte) (string, error) {
	q.mut.Lock()
	defer q.mut.Unlock()

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
	// In is an unbounded queue, that contains the names of the user. Chann is 2 goroutines with a slice in the middle.
	q.ch.In() <- name
	return name, err
}

// Next waits for a new entry to be added to the queue. It will block until an entry is returned or the context finished.
func (q *queue) Next(ctx context.Context, enc []byte) (map[string]string, []byte, string, error) {
	select {
	case name := <-q.ch.Out():
		buf, err := q.readFile(name, enc)
		defer q.delete(name)
		if err != nil {
			return nil, nil, "", err
		}
		r := &Record{}
		err = cbor.Unmarshal(buf, r)
		if err != nil {
			return nil, nil, "", err
		}
		return r.Meta, r.Data, name, nil
	case <-ctx.Done():
		q.ch.Close()
		return nil, nil, "", errors.New("context done")
	}
}

func (q *queue) Close() {
	q.ch.Close()
}

func (q *queue) delete(name string) {
	_ = os.Remove(name)
}

func (q *queue) writeFile(name string, data []byte) error {
	return os.WriteFile(name, data, 0644)
}

func (q *queue) readFile(name string, enc []byte) ([]byte, error) {
	bb, err := os.ReadFile(name)
	if err != nil {
		return enc, err
	}
	return bb, err
}
