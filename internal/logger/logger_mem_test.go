package logger

// 本文件锁定两处与「长期运行不涨内存/不涨 fd」相关的不变量。

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestClipForRingBoundsEntrySize 验证进入内存环的单条日志被截断。
//
// 环按条数封顶：不截断的话，一条 47 KB 的上游响应会让 1000 条的环
// 变成几十 MB 的常驻下限。
func TestClipForRingBoundsEntrySize(t *testing.T) {
	short := "hello"
	if got := clipForRing(short); got != short {
		t.Fatalf("short message was modified: %q", got)
	}

	huge := strings.Repeat("x", ringEntryMaxBytes*4)
	got := clipForRing(huge)
	if len(got) > ringEntryMaxBytes+64 {
		t.Fatalf("clipped length = %d, want <= %d(+suffix)", len(got), ringEntryMaxBytes)
	}
	if !strings.Contains(got, "环内截断") {
		t.Fatalf("clipped message lost the truncation marker: %q", got[len(got)-40:])
	}
}

// TestClipForRingKeepsUTF8Boundary 验证按字节截断不会切坏多字节字符。
func TestClipForRingKeepsUTF8Boundary(t *testing.T) {
	// 每个汉字 3 字节，构造一条必然切在字符串中间的消息。
	huge := strings.Repeat("汉", ringEntryMaxBytes)
	got := clipForRing(huge)
	body := strings.TrimSuffix(got, "…（环内截断，原长 "+itoa(len(huge))+" 字节）")
	for _, r := range body {
		if r == '\uFFFD' {
			t.Fatalf("clipped message contains a replacement rune (cut mid-rune)")
		}
	}
}

// TestOpenLogFileClosesPreviousSink 验证跨天轮转不会泄漏上一个文件句柄。
func TestOpenLogFileClosesPreviousSink(t *testing.T) {
	dir := t.TempDir()
	fileMu.Lock()
	fileSink = nil
	fileDay = ""
	fileDir = dir
	fileMu.Unlock()
	t.Cleanup(func() {
		fileMu.Lock()
		if fileSink != nil {
			_ = fileSink.Close()
			fileSink = nil
		}
		fileDay = ""
		fileMu.Unlock()
	})

	fileMu.Lock()
	if errOpen := openLogFileLocked(time.Date(2026, 11, 1, 23, 59, 0, 0, time.Local)); errOpen != nil {
		fileMu.Unlock()
		t.Fatalf("open day 1: %v", errOpen)
	}
	first := fileSink
	fileMu.Unlock()

	fileMu.Lock()
	if errOpen := openLogFileLocked(time.Date(2026, 11, 2, 0, 1, 0, 0, time.Local)); errOpen != nil {
		fileMu.Unlock()
		t.Fatalf("open day 2: %v", errOpen)
	}
	second := fileSink
	fileMu.Unlock()

	if first == second {
		t.Fatal("rotation reused the same file handle")
	}
	// 旧句柄必须已经关闭：再写会返回 os.ErrClosed。
	if _, errWrite := first.WriteString("probe"); errWrite == nil {
		t.Fatal("previous log file handle is still open after rotation (fd leak)")
	}

	// 两个文件都应该存在，证明确实发生了轮转而不是复用了同一个文件。
	for _, day := range []string{"2026-11-01", "2026-11-02"} {
		path := filepath.Join(dir, "freetier-"+day+".log")
		if _, errStat := os.Stat(path); errStat != nil {
			t.Fatalf("expected rotated file %s: %v", path, errStat)
		}
	}
}

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
