package types

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLabels(t *testing.T) {
	ts := &TimeSeries{}
	lbl := labels.FromStrings("one", "two")
	ts.AddLabels(lbl)
	require.Len(t, ts.Labels, len(lbl))

	ts.Labels = ts.Labels[:0]
	lbl = labels.FromStrings("one", "two", "three", "four")
	ts.AddLabels(lbl)
	require.Len(t, ts.Labels, len(lbl))

	lbl = labels.FromStrings()
	ts.AddLabels(lbl)
	require.Len(t, ts.Labels, len(lbl))

	lbl = labels.FromStrings("one", "two")
	ts.AddLabels(lbl)
	require.Len(t, ts.Labels, len(lbl))

}
