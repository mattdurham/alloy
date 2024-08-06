package filequeue

import (
	"context"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

func TestCorruption(t *testing.T) {
	dir := t.TempDir()
	log := log.NewNopLogger()
	q, err := NewQueue(dir, log)
	require.NoError(t, err)

	handle, err := q.Add(map[string]string{"name": "bob"}, []byte("first"))
	require.NoError(t, err)
	require.True(t, handle != "")

	handle, err = q.Add(map[string]string{"name": "bob"}, []byte("second"))
	require.NoError(t, err)
	require.True(t, handle != "")

	// First should be 1.committed
	fi, err := os.Stat(filepath.Join(dir, "1.committed"))
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(dir, fi.Name()), []byte("bad"), 0644)
	require.NoError(t, err)

	ctx := context.Background()
	buf := make([]byte, 0)
	// This should error, ideally callers should take the error and log it then move on.
	_, buf, _, err = q.Next(ctx, buf)
	require.Error(t, err)
	// This next one should be fine.
	_, buf, name, err := q.Next(ctx, buf)
	require.NoError(t, err)
	require.True(t, strings.Contains(name, "2.committed"))
	require.True(t, string(buf) == "second")
}

func TestFileDeleted(t *testing.T) {
	dir := t.TempDir()
	log := log.NewNopLogger()
	q, err := NewQueue(dir, log)
	require.NoError(t, err)

	evenHandles := make([]string, 0)
	for i := 0; i < 10; i++ {
		handle := addToQueue(t, q, nil, strconv.Itoa(i))
		if i%2 == 0 {
			evenHandles = append(evenHandles, handle)
		}
	}
	for _, h := range evenHandles {
		os.Remove(h)
	}
	buf := make([]byte, 0)
	// Every even file was deleted and should have an error.
	for i := 0; i < 10; i++ {
		_, buf2, _, err := q.Next(context.Background(), buf)
		if i%2 == 0 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.True(t, string(buf2) == strconv.Itoa(i))
		}
	}
}

func TestOtherFiles(t *testing.T) {
	dir := t.TempDir()
	log := log.NewNopLogger()
	q, err := NewQueue(dir, log)
	require.NoError(t, err)
	addToQueue(t, q, nil, "first")
	os.Create(filepath.Join(dir, "otherfile"))
	_, buf, handle, err := q.Next(context.Background(), make([]byte, 0))
	require.NoError(t, err)
	require.True(t, strings.Contains(handle, "1.committed"))
	require.True(t, string(buf) == "first")
}

func TestResuming(t *testing.T) {
	dir := t.TempDir()
	log := log.NewNopLogger()
	q, err := NewQueue(dir, log)
	require.NoError(t, err)
	addToQueue(t, q, nil, "first")
	addToQueue(t, q, nil, "second")
	q.Close()
	q2, err := NewQueue(dir, log)
	require.NoError(t, err)
	addToQueue(t, q2, nil, "third")
	_, buf, handle, err := q2.Next(context.Background(), make([]byte, 0))
	require.NoError(t, err)
	require.True(t, strings.Contains(handle, "1.committed"))
	require.True(t, string(buf) == "first")

	_, buf, handle, err = q2.Next(context.Background(), make([]byte, 0))
	require.NoError(t, err)
	require.True(t, strings.Contains(handle, "2.committed"))
	require.True(t, string(buf) == "second")

	_, buf, handle, err = q2.Next(context.Background(), make([]byte, 0))
	require.NoError(t, err)
	require.True(t, strings.Contains(handle, "3.committed"))
	require.True(t, string(buf) == "third")

}

func addToQueue(t *testing.T, q types.FileStorage, meta map[string]string, data string) string {
	handle, err := q.Add(meta, []byte(data))
	require.NoError(t, err)
	require.True(t, handle != "")
	return handle
}
