package types

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
