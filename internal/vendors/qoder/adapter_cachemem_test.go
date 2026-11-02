package qoder

// 本文件锁定 Bridge 缓存的回收行为。
//
// 背景（线上内存泄漏）：bridges 的键含凭证指纹，而凭证每 30 分钟续期就可能
// 换出新 token ⇒ 新键。旧的 Bridge 没人再 Load，只能靠主动清理；
// 缺了清理就是每条旧会话永久驻留（约 105 KB/条，含解析后的提示词模板），
// 与对话流量无关地每天每账号涨约 5 MB，直到把宿主进程撑爆。

import (
	"testing"
	"time"
)

// storeBridgeAt 直接写入一条缓存条目（跳过需要网络的建会话流程）。
func storeBridgeAt(v *adapter, key string, age time.Duration) {
	v.bridges.Store(key, bridgeEntry{
		bridge:      &Bridge{},
		fingerprint: key,
		createdAt:   time.Now().Add(-age),
	})
}

// ageBridges 把所有条目改写成「已存在 age」的形态，用来模拟时间流逝。
func ageBridges(v *adapter, age time.Duration) {
	v.bridges.Range(func(key, value any) bool {
		entry, okEntry := value.(bridgeEntry)
		if !okEntry {
			return true
		}
		entry.createdAt = time.Now().Add(-age)
		v.bridges.Store(key, entry)
		return true
	})
}

// TestBridgeCacheSweepDropsExpiredEntries 验证过期条目会被清掉。
func TestBridgeCacheSweepDropsExpiredEntries(t *testing.T) {
	v := &adapter{}
	storeBridgeAt(v, "cn|stale-1", qoderBridgeTTL+time.Minute)
	storeBridgeAt(v, "cn|stale-2", qoderBridgeTTL+time.Hour)
	if got := v.bridgeCacheSize(); got != 2 {
		t.Fatalf("precondition: cache size = %d, want 2", got)
	}

	v.sweepBridges()

	if got := v.bridgeCacheSize(); got != 0 {
		t.Fatalf("expired entries survived sweep: cache size = %d, want 0", got)
	}
}

// TestBridgeCacheSweepKeepsLiveEntries 验证仍在 TTL 内的条目不会被误删。
func TestBridgeCacheSweepKeepsLiveEntries(t *testing.T) {
	v := &adapter{}
	storeBridgeAt(v, "cn|live", time.Minute)

	v.sweepBridges()

	if got := v.bridgeCacheSize(); got != 1 {
		t.Fatalf("live entry was swept: cache size = %d, want 1", got)
	}
}

// TestBridgeCacheStaysBoundedAcrossRotations 是泄漏回归用例：
// 模拟 N 次凭证轮换（每轮旧条目已超过 TTL），缓存条数必须保持有界。
//
// 修复前：每轮新增一个键、旧键永不回收 ⇒ 缓存条数 == 轮数。
// 修复后：每轮清理一次 ⇒ 始终 ≤ 1。
func TestBridgeCacheStaysBoundedAcrossRotations(t *testing.T) {
	v := &adapter{}
	const rotations = 200

	for i := 0; i < rotations; i++ {
		// 上一轮建好的会话已经超过 TTL（真实场景：30 分钟后续期）。
		ageBridges(v, qoderBridgeTTL+time.Second)
		// 轮换出的新凭证 ⇒ 新键 ⇒ 新条目。
		storeBridgeAt(v, keyForRotation(i), 0)
		v.sweepBridges()

		if size := v.bridgeCacheSize(); size > 2 {
			t.Fatalf("cache grew with rotations: size = %d after %d rotations", size, i+1)
		}
	}

	if size := v.bridgeCacheSize(); size > 2 {
		t.Fatalf("final cache size = %d after %d rotations, want <= 2", size, rotations)
	}
}

// TestBridgeCacheSweepHandlesForeignValues 验证缓存里混入非 bridgeEntry 时不会 panic。
func TestBridgeCacheSweepHandlesForeignValues(t *testing.T) {
	v := &adapter{}
	v.bridges.Store("cn|foreign", "not-a-bridge-entry")

	v.sweepBridges()

	if got := v.bridgeCacheSize(); got != 0 {
		t.Fatalf("foreign entry survived sweep: cache size = %d, want 0", got)
	}
}

// keyForRotation 生成第 i 次轮换的缓存键（形如 bridgeCacheKey 的输出）。
func keyForRotation(i int) string {
	return "cn|rotation-" + string(rune('a'+i%26)) + "-" + itoa(i)
}

// itoa 是 strconv.Itoa 的极简替身，避免为一行测试引入依赖。
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
