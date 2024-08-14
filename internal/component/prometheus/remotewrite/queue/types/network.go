package types

import (
	"context"
)

type NetworkClient interface {
	Start()
	Stop()
	SendSeries(ctx context.Context, hash uint64, d *TimeSeries) error
	SendMetadata(ctx context.Context, d *MetaSeries) error
}
