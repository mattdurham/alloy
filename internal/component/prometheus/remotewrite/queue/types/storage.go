package types

import (
	"context"
)

type FileStorage interface {
	Start()
	Stop()
	Send(ctx context.Context, meta map[string]string, value []byte) error
}

type Serializer interface {
	Start()
	Stop()
	SendSeries(ctx context.Context, data []TimeSeries) error
	SendMetadata(ctx context.Context, data []MetaSeries) error
}
