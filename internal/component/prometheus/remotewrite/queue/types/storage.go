package types

import "context"

type FileStorage interface {
	// Add creates a file on the filesystem that is tagged with the meta data.
	Add(meta map[string]string, data []byte) (string, error)
	// Next reads from the WAL queue and will block until something is found or the context cancels.
	// Returns a map of tags that were passed in, data, name, and an error.
	// Then deletes the underlying file.
	Next(ctx context.Context, enc []byte) (map[string]string, []byte, string, error)
	// Close the underlying channels, any call to next will return and should not be called again.
	Close()
}
