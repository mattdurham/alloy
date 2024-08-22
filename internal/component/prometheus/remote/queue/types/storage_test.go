package types

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStorage(t *testing.T) {
	ts := GetTimeSeriesBinary()
	ts.Labels = labels.FromStrings("one", "two")
	ts.LabelsValues = make([]int32, 1)
	ts.LabelsNames = make([]int32, 1)
	ts.LabelsValues[0] = 1
	ts.LabelsNames[0] = 2

	PutTimeSeriesBinary(ts)
	ts = GetTimeSeriesBinary()
	require.Len(t, ts.Labels, 0)
	require.True(t, cap(ts.LabelsValues) == 1)
	require.True(t, cap(ts.LabelsNames) == 1)
	require.Len(t, ts.LabelsValues, 0)
	require.Len(t, ts.LabelsNames, 0)
}
