package types

import "context"

type WriteClient interface {
	// Queue will only return false if the underyling context is cancelled, else it will wait until it can queue.
	Queue(ctx context.Context, hash uint64, data []byte) bool
	// QueueMetadata will only return false if the underlying context is cancelled, else it will wait until it can queue.
	QueueMetadata(ctx context.Context, data []byte) bool
	// Stop drops all pending items and stops.
	Stop()
}
