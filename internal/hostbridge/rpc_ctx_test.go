package hostbridge

// 本文件锁定「宿主卡住时排队调用可以放弃」这一不变量。
//
// 修复前 acquire() 只有一个阻塞发送 `gate <- struct{}{}`：64 个槽位占满后，
// 新到的每一个请求都会连同它解码好的完整请求体一起永久停在门上——这是
// 唯一会随请求数无界增长、且宿主一变慢就触发的路径。

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// drainGate 清空所有已占用的宿主回调槽位。
func drainGate() {
	for {
		select {
		case <-gate:
		default:
			return
		}
	}
}

// fillGate 占满所有宿主回调槽位，并返回释放函数（无论测试里怎么消耗，最后都清空）。
func fillGate(t *testing.T) func() {
	t.Helper()
	for i := 0; i < maxConcurrentCalls; i++ {
		gate <- struct{}{}
	}
	if got := Inflight(); got != maxConcurrentCalls {
		t.Fatalf("precondition: inflight = %d, want %d", got, maxConcurrentCalls)
	}
	return drainGate
}

// TestCallCtxAbandonsQueueWhenContextExpires 验证槽位占满时，带超时的调用会退出。
func TestCallCtxAbandonsQueueWhenContextExpires(t *testing.T) {
	releaseSlots := fillGate(t)
	defer releaseSlots()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, errCall := CallCtx(ctx, "host.http.do", struct{}{})
	elapsed := time.Since(start)

	if !errors.Is(errCall, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", errCall)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("queued call took %s, want it to abandon at the deadline", elapsed)
	}
	// 放弃排队必须把准入计数还回去，否则关闭排空会永远等下去。
	if got := Inflight(); got != maxConcurrentCalls {
		t.Fatalf("inflight = %d after abandoned queue, want %d", got, maxConcurrentCalls)
	}
}

// TestCallCtxProceedsWhenSlotFrees 验证槽位空出后排队调用仍能正常执行。
func TestCallCtxProceedsWhenSlotFrees(t *testing.T) {
	Configure(func(callbackID, method string, payload any) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})
	t.Cleanup(func() { Configure(nil) })
	releaseSlots := fillGate(t)
	defer releaseSlots()

	freed := make(chan struct{})
	go func() {
		<-gate // 模拟一个在飞回调返回，腾出槽位
		close(freed)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, errCall := CallCtx(ctx, "host.http.do", struct{}{}); errCall != nil {
		t.Fatalf("call after slot freed: %v", errCall)
	}
	<-freed
}
