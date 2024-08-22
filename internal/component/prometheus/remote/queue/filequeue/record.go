package filequeue

// Record wraps the input data and combines it with the metadata.
//
//go:generate msgp
type Record struct {
	Meta map[string]string
	Data []byte
}
