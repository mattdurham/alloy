package filequeue

import (
	"context"
	"github.com/go-kit/log"
	"github.com/grafana/alloy/internal/component/prometheus/remotewrite/queue/types"
	"github.com/stretchr/testify/require"
	"github.com/vladopajic/go-actor/actor"
	"testing"
	"time"
)

func BenchmarkFileQueue(t *testing.B) {
	for i := 0; i < t.N; i++ {
		dir := t.TempDir()
		log := log.NewNopLogger()
		mbx := actor.NewMailbox[types.DataHandle]()
		mbx.Start()
		defer mbx.Stop()
		q, err := NewQueue(dir, func(ctx context.Context, dh types.DataHandle) {
			_ = mbx.Send(ctx, dh)
		}, log)
		require.NoError(t, err)
		q.Start()
		defer q.Stop()
		err = q.Send(context.Background(), nil, []byte("test"))

		require.NoError(t, err)

		meta, buf, err := getHandleBench(mbx)
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
}

func getHandleBench(mbx actor.MailboxReceiver[types.DataHandle]) (map[string]string, []byte, error) {
	select {

	case item, _ := <-mbx.ReceiveC():
		return item.Get()
	}

}
