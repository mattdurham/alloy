package types

import "context"

type WriteClient interface {
	// Queue will only return false if the underyling context is cancelled, else it will wait until it can queue.
	Queue(ctx context.Context, d *Item) bool
	// QueueMetadata will only return false if the underlying context is cancelled, else it will wait until it can queue.
	QueueMetadata(ctx context.Context, d *Item) bool

	Drain(ctx context.Context) []*Item
	DrainMetadata(ctx context.Context) []*Item
}

type Item struct {
	Hash  uint64
	Bytes []byte
}
