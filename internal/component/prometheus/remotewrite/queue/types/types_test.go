package types

import (
	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEncoding(t *testing.T) {
	seriesCount := 1_000
	sg := &SeriesGroup{
		Series: make([]*TimeSeriesBinary, seriesCount),
	}
	for i := 0; i < seriesCount; i++ {
		sg.Series[i] = &TimeSeriesBinary{
			LabelsNames:  make([]int32, 100),
			LabelsValues: make([]int32, 100),
		}
		for j := 0; j < 100; j++ {
			sg.Series[i].LabelsNames[j] = int32(j)
			sg.Series[i].LabelsValues[j] = int32(j)
		}
	}

	buf, err := cbor.Marshal(sg)
	require.NoError(t, err)
	sg, err = DeserializeToSeriesGroup(sg, buf)
	require.NoError(t, err)
	testUnmarshal(t, sg, 1_000, 100)
	// Theoretically this should do a lot less allocs
	sg, err = DeserializeToSeriesGroup(sg, buf)
	require.NoError(t, err)
}

func testUnmarshal(t *testing.T, sg *SeriesGroup, count, max int) {
	require.Len(t, sg.Series, count)
	for _, s := range sg.Series {
		for j := 0; j < max; j++ {
			require.True(t, s.LabelsValues[j] == int32(j))
			require.True(t, s.LabelsNames[j] == int32(j))
		}
	}
}
