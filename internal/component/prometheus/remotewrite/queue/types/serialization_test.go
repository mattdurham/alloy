package types

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestLabels(t *testing.T) {
	lblsMap := make(map[string]string)
	unique := make(map[string]struct{})
	for i := 0; i < 1_000; i++ {
		k := fmt.Sprintf("key_%d", i)
		v := randString()
		lblsMap[k] = v
		unique[k] = struct{}{}
		unique[v] = struct{}{}
	}
	sg := &SeriesGroup{
		Series: make([]*TimeSeriesBinary, 1),
	}
	sg.Series[0] = GetTimeSeriesBinary()
	defer PutTimeSeriesBinary(sg.Series[0])
	sg.Series[0].Labels = labels.FromMap(lblsMap)
	strMap := make(map[string]int32)

	index := int32(0)
	index = FillBinary(sg.Series[0], strMap, index)
	require.True(t, index == int32(len(unique)))
	stringsSlice := make([]string, len(strMap))
	for k, v := range strMap {
		stringsSlice[v] = k
	}
	sg.Strings = stringsSlice
	buf, err := sg.MarshalMsg(nil)
	require.NoError(t, err)
	newSg := &SeriesGroup{}
	newSg, _, err = DeserializeToSeriesGroup(newSg, buf)
	require.NoError(t, err)
	series1 := newSg.Series[0]
	series2 := sg.Series[0]
	require.Len(t, series2.Labels, len(series1.Labels))
	for i, lbl := range series2.Labels {
		require.Equal(t, lbl.Name, series1.Labels[i].Name)
		require.Equal(t, lbl.Value, series1.Labels[i].Value)
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString() string {
	b := make([]rune, rand.Intn(20))
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
