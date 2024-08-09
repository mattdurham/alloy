package types

import (
	"context"
)

type NetworkClient interface {
	Start()
	Stop()
	SendSeries(ctx context.Context, hash uint64, data []byte) error
	SendMetadata(ctx context.Context, data []byte) error
}
