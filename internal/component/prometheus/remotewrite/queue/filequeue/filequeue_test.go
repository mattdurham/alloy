package filequeue

import (
	"context"
	"fmt"
	"github.com/vladopajic/go-actor/actor"
	"go.uber.org/goleak"
	"testing"
	"time"

	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestFileQueue(t *testing.T) {
	defer goleak.VerifyNone(t)
	dir := t.TempDir()
	log := log.NewNopLogger()
	mbx := actor.NewMailbox[types.DataHandle]()
	mbx.Start()
	defer mbx.Stop()
	q, err := NewQueue(dir, mbx, log)
	require.NoError(t, err)
	defer q.Stop()
	err = q.Mailbox().Send(context.Background(), types.Data{
		Meta: nil,
		Data: []byte("test"),
	})
	require.NoError(t, err)

	meta, buf, err := getHandle(t, mbx)
	require.NoError(t, err)
	require.True(t, string(buf) == "test")
	require.Len(t, meta, 0)

	// Ensure nothing new comes through.
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case <-timer.C:
		return
	case <-mbx.ReceiveC():
		require.True(t, false)
	}
}

// This example will demonstrate how to create actors for producer-consumer use case.
// Producer will create incremented number on every 1 second interval and
// consumer will print whatever number it receives.
func TestActor(t *testing.T) {
	mbx := actor.NewMailbox[int]()

	// Produce and consume workers are created with same mailbox
	// so that produce worker can send messages directly to consume worker
	p := actor.New(&producerWorker{mailbox: mbx})
	c1 := actor.New(&consumerWorker{mailbox: mbx, id: 1})

	// Note: Example creates two consumers for the sake of demonstration
	// since having one or more consumers will produce the same result.
	// Message on stdout will be written by first consumer that reads from mailbox.
	c2 := actor.New(&consumerWorker{mailbox: mbx, id: 2})

	// Combine all actors to singe actor so we can start and stop all at once
	a := actor.Combine(mbx, p, c1, c2).Build()
	a.Start()
	defer a.Stop()

	// Stdout output:
	// consumed 1      (worker 1)
	// consumed 2      (worker 2)
	// consumed 3      (worker 1)
	// consumed 4      (worker 2)
	// ...

	timer := time.NewTicker(5 * time.Second)
	<-timer.C
}

// producerWorker will produce incremented number on 1 second interval
type producerWorker struct {
	mailbox actor.MailboxSender[int]
	num     int
}

func (w *producerWorker) DoWork(ctx actor.Context) actor.WorkerStatus {
	select {
	case <-ctx.Done():
		return actor.WorkerEnd

	case <-time.After(time.Second):
		w.num++
		w.mailbox.Send(ctx, w.num)

		return actor.WorkerContinue
	}
}

// consumerWorker will consume numbers received on mailbox
type consumerWorker struct {
	mailbox actor.MailboxReceiver[int]
	id      int
}

func (w *consumerWorker) DoWork(ctx actor.Context) actor.WorkerStatus {
	select {
	case <-ctx.Done():
		return actor.WorkerEnd

	case num := <-w.mailbox.ReceiveC():
		fmt.Printf("consumed %d \t(worker %d)\n", num, w.id)

		return actor.WorkerContinue
	}
}

//
//func TestMetaFileQueue(t *testing.T) {
//	dir := t.TempDir()
//	log := log.NewNopLogger()
//	q, err := NewQueue(dir, log)
//	require.NoError(t, err)
//
//	handle, err := q.Add(map[string]string{"name": "bob"}, []byte("test"))
//	require.NoError(t, err)
//	require.True(t, handle != "")
//
//	ctx := context.Background()
//	buf := make([]byte, 0)
//	meta, buf, name, err := q.Next(ctx, buf)
//	require.NoError(t, err)
//	require.NotNil(t, meta)
//	require.True(t, meta["name"] == "bob")
//	require.True(t, name != "")
//	require.True(t, string(buf) == "test")
//}
//
//func TestCorruption(t *testing.T) {
//	dir := t.TempDir()
//	log := log.NewNopLogger()
//	q, err := NewQueue(dir, log)
//	require.NoError(t, err)
//
//	handle, err := q.Add(map[string]string{"name": "bob"}, []byte("first"))
//	require.NoError(t, err)
//	require.True(t, handle != "")
//
//	handle, err = q.Add(map[string]string{"name": "bob"}, []byte("second"))
//	require.NoError(t, err)
//	require.True(t, handle != "")
//
//	// First should be 1.committed
//	fi, err := os.Stat(filepath.Join(dir, "1.committed"))
//	require.NoError(t, err)
//	err = os.WriteFile(filepath.Join(dir, fi.Name()), []byte("bad"), 0644)
//	require.NoError(t, err)
//
//	ctx := context.Background()
//	buf := make([]byte, 0)
//	// This should error, ideally callers should take the error and log it then move on.
//	_, buf, _, err = q.Next(ctx, buf)
//	require.Error(t, err)
//	// This next one should be fine.
//	_, buf, name, err := q.Next(ctx, buf)
//	require.NoError(t, err)
//	require.True(t, strings.Contains(name, "2.committed"))
//	require.True(t, string(buf) == "second")
//}
//
//func TestFileDeleted(t *testing.T) {
//	dir := t.TempDir()
//	log := log.NewNopLogger()
//	q, err := NewQueue(dir, log)
//	require.NoError(t, err)
//
//	evenHandles := make([]string, 0)
//	for i := 0; i < 10; i++ {
//		handle := addToQueue(t, q, nil, strconv.Itoa(i))
//		if i%2 == 0 {
//			evenHandles = append(evenHandles, handle)
//		}
//	}
//	for _, h := range evenHandles {
//		os.Remove(h)
//	}
//	buf := make([]byte, 0)
//	// Every even file was deleted and should have an error.
//	for i := 0; i < 10; i++ {
//		_, buf2, _, err := q.Next(context.Background(), buf)
//		if i%2 == 0 {
//			require.Error(t, err)
//		} else {
//			require.NoError(t, err)
//			require.True(t, string(buf2) == strconv.Itoa(i))
//		}
//	}
//}
//
//func TestOtherFiles(t *testing.T) {
//	dir := t.TempDir()
//	log := log.NewNopLogger()
//	q, err := NewQueue(dir, log)
//	require.NoError(t, err)
//	addToQueue(t, q, nil, "first")
//	os.Create(filepath.Join(dir, "otherfile"))
//	_, buf, handle, err := q.Next(context.Background(), make([]byte, 0))
//	require.NoError(t, err)
//	require.True(t, strings.Contains(handle, "1.committed"))
//	require.True(t, string(buf) == "first")
//}
//
//func TestResuming(t *testing.T) {
//	dir := t.TempDir()
//	log := log.NewNopLogger()
//	q, err := NewQueue(dir, log)
//	require.NoError(t, err)
//	addToQueue(t, q, nil, "first")
//	addToQueue(t, q, nil, "second")
//	q.Close()
//	q2, err := NewQueue(dir, log)
//	require.NoError(t, err)
//	addToQueue(t, q2, nil, "third")
//	_, buf, handle, err := q2.Next(context.Background(), make([]byte, 0))
//	require.NoError(t, err)
//	require.True(t, strings.Contains(handle, "1.committed"))
//	require.True(t, string(buf) == "first")
//
//	_, buf, handle, err = q2.Next(context.Background(), make([]byte, 0))
//	require.NoError(t, err)
//	require.True(t, strings.Contains(handle, "2.committed"))
//	require.True(t, string(buf) == "second")
//
//	_, buf, handle, err = q2.Next(context.Background(), make([]byte, 0))
//	require.NoError(t, err)
//	require.True(t, strings.Contains(handle, "3.committed"))
//	require.True(t, string(buf) == "third")
//
//}
/*
func addToQueue(t *testing.T, q types.FileStorage, meta map[string]string, data string) string {
	handle, err := q.Add(meta, []byte(data))
	require.NoError(t, err)
	require.True(t, handle != "")
	return handle
}
*/
func getHandle(t *testing.T, mbx actor.MailboxReceiver[types.DataHandle]) (map[string]string, []byte, error) {
	timer := time.NewTicker(5 * time.Second)
	select {
	case <-timer.C:
		require.True(t, false)
		// This is only here to satisfy the linting.
		return nil, nil, nil
	case item, ok := <-mbx.ReceiveC():
		require.True(t, ok)
		return item.Get(item.Name)
	}

}
