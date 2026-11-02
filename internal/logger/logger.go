// Package logger 提供插件内日志：内存环 + stderr + 可选按天文件。
//
// 三处刻意的设计：
//   - sink 用 stderr 而非 stdout：动态库被 CPA 宿主加载，stdout 属于宿主进程，
//     往 stdout 写会污染宿主的正常输出；
//   - 内存环供管理页按 since 增量拉取最近日志（宿主日志里翻插件日志很不方便）；
//   - 按天文件可选，默认关闭，只保留最近 N 天。
//
// 安全约定：调用方不得把完整凭证写入日志，只允许输出前缀。
package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Level 是日志级别。数值越大越严重。
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelError
)

// ringCapacity 是内存环容量。管理页默认一次拉 200 条，1000 条足够回溯。
const ringCapacity = 1000

// ringEntryMaxBytes 是单条日志进入内存环时的上限。
//
// 环按「条数」封顶，而调用方会打出整段上游响应（模型清单 47 KB、整包
// 请求体等）。1000 条 × 单条几十 KB 就是几十 MB 的常驻下限——在 1.6 GB
// 的机器上这就是 OOM 的最后一根稻草。这里只截断环里的副本，stderr 与
// 文件仍写完整内容（排障时看文件）。
const ringEntryMaxBytes = 8 << 10

// fileRetentionDays 是按天日志文件的保留天数。
const fileRetentionDays = 7

// Entry 是一条日志。
type Entry struct {
	Seq     uint64 `json:"seq"`
	Time    string `json:"time"`
	Level   string `json:"level"`
	Message string `json:"message"`
}

type ring struct {
	mu      sync.Mutex
	entries []Entry
	head    int
	full    bool
	nextSeq uint64
}

var (
	globalRing = &ring{entries: make([]Entry, 0, ringCapacity)}

	levelMu    sync.RWMutex
	currentLvl = LevelInfo

	fileMu   sync.Mutex
	fileSink *os.File
	fileDay  string
	fileDir  string
)

// SetLevel 设置日志级别。接受 debug / info / error，未知值回落 info。
func SetLevel(raw string) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		setLevel(LevelDebug)
	case "error":
		setLevel(LevelError)
	default:
		setLevel(LevelInfo)
	}
}

func setLevel(lvl Level) {
	levelMu.Lock()
	currentLvl = lvl
	levelMu.Unlock()
}

// CurrentLevel 返回当前级别名（供管理页展示）。
func CurrentLevel() string {
	levelMu.RLock()
	defer levelMu.RUnlock()
	switch currentLvl {
	case LevelDebug:
		return "debug"
	case LevelError:
		return "error"
	default:
		return "info"
	}
}

func enabled(lvl Level) bool {
	levelMu.RLock()
	defer levelMu.RUnlock()
	return lvl >= currentLvl
}

// InitFile 开启或关闭文件 sink。
//
// enabled 为 false 时关闭已打开的文件；为 true 时在 dir 下按天写入。
// 失败不致命：日志写不进去不应该让插件拒绝启动。
func InitFile(enabled bool, dir string) error {
	fileMu.Lock()
	defer fileMu.Unlock()

	if fileSink != nil {
		if errClose := fileSink.Close(); errClose != nil {
			fmt.Fprintf(os.Stderr, "[freetier] close log file failed: %v\n", errClose)
		}
		fileSink = nil
		fileDay = ""
	}
	if !enabled {
		return nil
	}
	if errMkdir := os.MkdirAll(dir, 0o755); errMkdir != nil {
		return fmt.Errorf("create log dir %s: %w", dir, errMkdir)
	}
	fileDir = dir
	pruneLogFiles(dir)
	return openLogFileLocked(time.Now())
}

// openLogFileLocked 打开当天的日志文件。调用方必须持有 fileMu。
func openLogFileLocked(now time.Time) error {
	day := now.Format("2006-01-02")
	path := filepath.Join(fileDir, "freetier-"+day+".log")
	file, errOpen := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if errOpen != nil {
		return fmt.Errorf("open log file %s: %w", path, errOpen)
	}
	// 跨天轮转也走这里：先把上一天的句柄关掉再替换，否则每天泄漏一个 fd
	// （长期运行后进程 fd 只增不减，还会让旧文件无法删除/回收）。
	if fileSink != nil && fileSink != file {
		if errClose := fileSink.Close(); errClose != nil {
			fmt.Fprintf(os.Stderr, "[freetier] close previous log file failed: %v\n", errClose)
		}
	}
	fileSink = file
	fileDay = day
	return nil
}

// clipForRing 截断进入内存环的单条日志。
//
// 只截环里的副本：stderr 与文件写的是完整内容。
func clipForRing(message string) string {
	if len(message) <= ringEntryMaxBytes {
		return message
	}
	// 按字节截断可能切在多字节字符中间，退到上一个合法边界。
	cut := ringEntryMaxBytes
	for cut > 0 && !utf8.RuneStart(message[cut]) {
		cut--
	}
	return message[:cut] + fmt.Sprintf("…（环内截断，原长 %d 字节）", len(message))
}

// pruneLogFiles 删除超过保留期的日志文件。
func pruneLogFiles(dir string) {
	entries, errRead := os.ReadDir(dir)
	if errRead != nil {
		return
	}
	cutoff := time.Now().AddDate(0, 0, -fileRetentionDays)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "freetier-") {
			continue
		}
		info, errInfo := entry.Info()
		if errInfo != nil || info.ModTime().After(cutoff) {
			continue
		}
		names = append(names, filepath.Join(dir, entry.Name()))
	}
	sort.Strings(names)
	for _, name := range names {
		_ = os.Remove(name)
	}
}

// Close 关闭文件 sink。供 shutdown 调用。
func Close() {
	fileMu.Lock()
	defer fileMu.Unlock()
	if fileSink != nil {
		_ = fileSink.Close()
		fileSink = nil
		fileDay = ""
	}
}

// Debug 记录一条 debug 日志。
func Debug(format string, args ...any) { log(LevelDebug, "DEBUG", format, args...) }

// Info 记录一条 info 日志。
func Info(format string, args ...any) { log(LevelInfo, "INFO", format, args...) }

// Error 记录一条 error 日志。
func Error(format string, args ...any) { log(LevelError, "ERROR", format, args...) }

func log(lvl Level, label string, format string, args ...any) {
	if !enabled(lvl) {
		return
	}
	message := format
	if len(args) > 0 {
		message = fmt.Sprintf(format, args...)
	}
	now := time.Now()
	line := fmt.Sprintf("%s [%s] %s\n", now.Format("2006-01-02 15:04:05.000"), label, message)

	fmt.Fprint(os.Stderr, line)

	globalRing.append(Entry{
		Time:    now.Format("2006-01-02 15:04:05.000"),
		Level:   strings.ToLower(label),
		Message: clipForRing(message),
	})

	fileMu.Lock()
	if fileSink != nil {
		// 跨天时先切文件再写。
		if day := now.Format("2006-01-02"); day != fileDay {
			if errRotate := openLogFileLocked(now); errRotate != nil {
				fmt.Fprintf(os.Stderr, "[freetier] rotate log file failed: %v\n", errRotate)
			}
		}
		if fileSink != nil {
			_, _ = fileSink.WriteString(line)
		}
	}
	fileMu.Unlock()
}

func (r *ring) append(entry Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextSeq++
	entry.Seq = r.nextSeq
	if len(r.entries) < ringCapacity {
		r.entries = append(r.entries, entry)
		return
	}
	r.entries[r.head] = entry
	r.head = (r.head + 1) % ringCapacity
	r.full = true
}

func (r *ring) orderedEntries() []Entry {
	if !r.full {
		out := make([]Entry, len(r.entries))
		copy(out, r.entries)
		return out
	}
	out := make([]Entry, ringCapacity)
	n := copy(out, r.entries[r.head:])
	copy(out[n:], r.entries[:r.head])
	return out
}

// Snapshot 返回 seq 大于 since 的日志（最多 limit 条）与当前最大 seq。
func Snapshot(since uint64, limit int) ([]Entry, uint64) {
	if limit <= 0 || limit > ringCapacity {
		limit = 200
	}
	globalRing.mu.Lock()
	defer globalRing.mu.Unlock()

	newest := globalRing.nextSeq
	all := globalRing.orderedEntries()
	out := make([]Entry, 0, limit)
	if since == 0 {
		start := len(all) - limit
		if start < 0 {
			start = 0
		}
		out = append(out, all[start:]...)
		return out, newest
	}
	for _, entry := range all {
		if entry.Seq <= since {
			continue
		}
		if len(out) >= limit {
			break
		}
		out = append(out, entry)
	}
	return out, newest
}
