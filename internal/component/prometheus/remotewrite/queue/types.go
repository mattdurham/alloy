package queue

import (
	"fmt"
	"time"

	"github.com/prometheus/prometheus/storage"
)

func defaultArgs() Arguments {
	return Arguments{
		TTL:               2 * time.Hour,
		BatchSizeBytes:    32 * 1024 * 1024,
		FlushTime:         5 * time.Second,
		AppenderBatchSize: 1_000,
		Connection: ConnectionConfig{
			Timeout:                 15 * time.Second,
			RetryBackoff:            1 * time.Second,
			MaxRetryBackoffAttempts: 0,
			BatchCount:              1_000,
			FlushDuration:           1 * time.Second,
			QueueCount:              4,
		},
	}
}

type Arguments struct {
	// TTL is how old a series can be.
	TTL time.Duration `alloy:"ttl,attr,optional"`
	// The batch size to persist to the file queue.
	BatchSizeBytes int `alloy:"batch_size_bytes,attr,optional"`
	// How often to flush to the file queue if BatchSizeBytes isn't met.
	FlushTime  time.Duration    `alloy:"flush_time,attr,optional"`
	Connection ConnectionConfig `alloy:"endpoint,block"`
	// AppenderBatchSize determines how often to flush the appender batch size.
	AppenderBatchSize int `alloy:"appender_batch_size,attr,optional"`
}

type ConnectionConfig struct {
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

func (r *Arguments) Validate() error {
	if r.AppenderBatchSize == 0 {
		return fmt.Errorf("appender_batch_size must be greater than zero")
	}
	if r.Connection.BatchCount <= 0 {
		return fmt.Errorf("batch_count must be greater than 0")
	}
	return nil
}
