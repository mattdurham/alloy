package cbor

import (
	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/go-kit/log"
	"github.com/grafana/alloy/internal/alloy/logging/level"
	"math"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/filequeue"
)

type Raw struct {
	_     struct{} `cbor:",toarray"`
	Hash  uint64   `cbor:"1,keyasint"`
	Bytes []byte   `cbor:"2,keyasint"`
	TS    int64    `cbor:"3,keyasint"`
}

type SeriesGroup struct {
	_        struct{} `cbor:",toarray"`
	Series   []*Raw   `cbor:"1,keyasint"`
	Metadata []*Raw   `cbor:"2,keyasint"`
}

func DeserializeToSeriesGroup(buf []byte) (*SeriesGroup, error) {
	sg := &SeriesGroup{}
	decOpt := cbor.DecOptions{
		MaxArrayElements: math.MaxInt32,
	}
	dec, err := decOpt.DecMode()
	if err != nil {
		return nil, err
	}
	err = dec.Unmarshal(buf, sg)
	return sg, err
}

type Serializer struct {
	mut           sync.RWMutex
	maxSizeBytes  int
	flushDuration time.Duration
	queue         filequeue.Storage
	group         *SeriesGroup
	lastFlush     time.Time
	bytesInGroup  uint32
	logger        log.Logger
}

func NewSerializer(maxSizeBytes int, flushDuration time.Duration, q filequeue.Storage, l log.Logger) (*Serializer, error) {
	return &Serializer{
		maxSizeBytes:  maxSizeBytes,
		flushDuration: flushDuration,
		queue:         q,
		group: &SeriesGroup{
			Series:   make([]*Raw, 0),
			Metadata: make([]*Raw, 0),
		},
		logger: l,
	}, nil
}

func (s *Serializer) AppendMetadata(data []*Raw) error {
	if len(data) == 0 {
		return nil
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	for _, d := range data {
		s.group.Metadata = append(s.group.Series, d)
		s.bytesInGroup = s.bytesInGroup + uint32(len(d.Bytes)) + 4
	}
	// If we would go over the max size then send, or if we have hit the flush duration then send.
	if s.bytesInGroup > uint32(s.maxSizeBytes) {
		level.Debug(s.logger).Log("flushing to disk due to maxSizeBytes", s.maxSizeBytes)
		return s.store()
	} else if time.Since(s.lastFlush) > s.flushDuration {
		return s.store()
	}
	return nil
}

func (s *Serializer) Append(data []*Raw) error {
	if len(data) == 0 {
		return nil
	}
	s.mut.Lock()
	defer s.mut.Unlock()

	for _, d := range data {
		s.group.Series = append(s.group.Series, d)
		s.bytesInGroup = s.bytesInGroup + uint32(len(d.Bytes)) + 4
	}
	// If we would go over the max size then send, or if we have hit the flush duration then send.
	if s.bytesInGroup > uint32(s.maxSizeBytes) {
		level.Debug(s.logger).Log("flushing to disk due to maxSizeBytes", s.maxSizeBytes)
		return s.store()
	} else if time.Since(s.lastFlush) > s.flushDuration {
		return s.store()
	}
	return nil
}

func (s *Serializer) Update(flushDuration time.Duration, batchBytes int) {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.flushDuration = flushDuration
	s.maxSizeBytes = batchBytes
}

func (s *Serializer) store() error {
	s.lastFlush = time.Now()

	buffer, err := cbor.Marshal(s.group)
	// We can reset the group now.
	s.group = &SeriesGroup{
		Series:   make([]*Raw, 0),
		Metadata: make([]*Raw, 0),
	}

	if err != nil {
		// Something went wrong with serializing the whole group so lets drop it.
		return err
	}
	out := snappy.Encode(buffer)
	_, err = s.queue.Add(nil, out)
	s.bytesInGroup = 0
	return err
}
