package types

import (
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/prometheus/prometheus/storage"
)

type SeriesGroup struct {
	_        struct{} `cbor:",toarray"`
	Series   []Raw    `cbor:"1,keyasint"`
	Metadata []Raw    `cbor:"2,keyasint"`
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

type Raw struct {
	_     struct{} `cbor:",toarray"`
	Hash  uint64   `cbor:"1,keyasint"`
	Bytes []byte   `cbor:"2,keyasint"`
	TS    int64    `cbor:"3,keyasint"`
}

type RawMetadata struct {
	Raw
}

func defaultArgs() Arguments {
	return Arguments{
		TTL:               2 * time.Hour,
		BatchSizeBytes:    32 * 1024 * 1024,
		FlushDuration:     5 * time.Second,
		AppenderBatchSize: 1_000,
	}
}

type Arguments struct {
	// TTL is how old a series can be.
	TTL time.Duration `alloy:"ttl,attr,optional"`
	// The batch size to persist to the file queue.
	BatchSizeBytes int `alloy:"batch_size_bytes,attr,optional"`
	// How often to flush to the file queue if BatchSizeBytes isn't met.
	FlushDuration time.Duration      `alloy:"flush_duration,attr,optional"`
	Connections   []ConnectionConfig `alloy:"endpoint,block"`
	// AppenderBatchSize determines how often to flush the appender batch size.
	AppenderBatchSize int               `alloy:"appender_batch_size,attr,optional"`
	ExternalLabels    map[string]string `alloy:"external_labels,attr,optional"`
}

func (a Arguments) TriggerSerializationChange(b Arguments) bool {
	if a.TTL != b.TTL {
		return true
	}
	if a.BatchSizeBytes != b.BatchSizeBytes {
		return true
	}
	if a.FlushDuration != b.FlushDuration {
		return true
	}
	return true
}

func (a Arguments) TriggerWriteClientChange(b Arguments) bool {
	return reflect.DeepEqual(a.Connections, b.Connections)
}

type ConnectionConfig struct {
	Name      string        `alloy:",label"`
	URL       string        `alloy:"url,attr"`
	BasicAuth BasicAuth     `alloy:"basic_auth,block,optional"`
	Timeout   time.Duration `alloy:"write_timeout,attr,optional"`
	// How long to wait between retries.
	RetryBackoff time.Duration `alloy:"retry_backoff,attr,optional"`
	// Maximum number of retries.
	MaxRetryBackoffAttempts int `alloy:"max_retry_backoff,attr,optional"`
	// How many series to write at a time.
	BatchCount int `alloy:"batch_count,attr,optional"`
	// How long to wait before sending regardless of batch count.
	FlushDuration time.Duration `alloy:"flush_duration,attr,optional"`
	// How many concurrent queues to have.
	QueueCount uint `alloy:"queue_count,attr,optional"`

	ExternalLabels map[string]string `alloy:"external_labels,attr,optional"`
}

type BasicAuth struct {
	Username string `alloy:"username,attr,optional"`
	Password string `alloy:"password,attr,optional"`
}

type Exports struct {
	Receiver storage.Appendable `alloy:"receiver,attr"`
}

// SetToDefault sets the default
func (rc *Arguments) SetToDefault() {
	*rc = defaultArgs()
}
func defaultCC() ConnectionConfig {
	return ConnectionConfig{
		Timeout:                 15 * time.Second,
		RetryBackoff:            1 * time.Second,
		MaxRetryBackoffAttempts: 0,
		BatchCount:              1_000,
		FlushDuration:           1 * time.Second,
		QueueCount:              4,
	}
}
func (cc *ConnectionConfig) SetToDefault() {
	*cc = defaultCC()
}

func (r *Arguments) Validate() error {
	if r.AppenderBatchSize == 0 {
		return fmt.Errorf("appender_batch_size must be greater than zero")
	}
	for _, conn := range r.Connections {
		if conn.BatchCount <= 0 {
			return fmt.Errorf("batch_count must be greater than 0")
		}
	}
	return nil
}
