package filequeue

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestFileQueue(t *testing.T) {
	dir := t.TempDir()
	log := log.NewNopLogger()
	q, err := NewQueue(dir, log)
	require.NoError(t, err)
	handle, err := q.Add(nil, []byte("test"))
	require.NoError(t, err)
	require.True(t, handle != "")

	ctx := context.Background()
	buf := make([]byte, 0)
	meta, buf, name, err := q.Next(ctx, buf)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, name != "")
	require.True(t, string(buf) == "test")

	ctx, cncl := context.WithTimeout(ctx, 1*time.Second)
	defer cncl()
	meta, buf, name, err = q.Next(ctx, buf)
	require.Error(t, err)
	require.Nil(t, meta)
	require.True(t, len(buf) == 0)
	require.True(t, name == "")
}

func TestMetaFileQueue(t *testing.T) {
	dir := t.TempDir()
	log := log.NewNopLogger()
	q, err := NewQueue(dir, log)
	require.NoError(t, err)

	handle, err := q.Add(map[string]string{"name": "bob"}, []byte("test"))
	require.NoError(t, err)
	require.True(t, handle != "")

	ctx := context.Background()
	buf := make([]byte, 0)
	meta, buf, name, err := q.Next(ctx, buf)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta["name"] == "bob")
	require.True(t, name != "")
	require.True(t, string(buf) == "test")
}
