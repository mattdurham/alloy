package types

import "time"

type SerializerConfig struct {
	MaxSignalsInBatch uint32
	FlushFrequency    time.Duration
}
