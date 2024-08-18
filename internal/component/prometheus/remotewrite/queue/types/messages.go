package types

// Below are the types of messages passed between the layers.

type QueueItem struct {
	Buffer []byte
	Meta   map[string]string
}

type NetworkQueueItem struct {
	Hash uint64
	TS   *TimeSeriesBinary
}

type NetworkMetadataItem struct {
	TS *MetaSeriesBinary
}

type Data struct {
	Meta map[string]string
	Data []byte
}

type DataHandle struct {
	Name string
	Get  func() (map[string]string, []byte, error)
}
