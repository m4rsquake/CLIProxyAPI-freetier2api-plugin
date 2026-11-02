//go:build soak

package tests

// 泄漏压测：把插件的热点链路跑 N 轮，每轮后强制 GC 并记录堆占用。
//
// 目的：内存泄漏在真实宿主里表现为进程 RSS 单调上涨直到 OOM；在插件内部
// 则必须表现为「强制 GC 之后仍会保留」的对象。所以这里只认 GC 之后的
// HeapAlloc / HeapObjects 趋势，不看瞬时值。
//
// 用法（不要进 CI）：
//
//	go test -tags soak ./tests/integration -run TestSoakLeak -v -timeout 30m \
//	  -args -soak.workload=normalize -soak.iterations=20000 -soak.every=4000
//
// 判读：HeapAlloc（GC 后）随轮数单调上涨就是有东西被长期持有；
// 需要证据时加 -memprofile /tmp/leak.mem 再 `go tool pprof -top -sample_index=inuse_space`。

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"testing"

	"freetier2api-plugin/cpasdk/pluginabi"
	"freetier2api-plugin/cpasdk/pluginapi"
	"freetier2api-plugin/internal/app"
)

var (
	soakIterations = flag.Int("soak.iterations", 20000, "总轮数")
	soakEvery      = flag.Int("soak.every", 4000, "每多少轮采样一次")
	soakWorkload   = flag.String("soak.workload", "exec", "exec | stream | normalize | models")
	soakBodyBytes  = flag.Int("soak.body", 64<<10, "normalize 负载的每轮请求体字节数")
)

// sampleHeap 返回强制 GC 之后的堆统计。
func sampleHeap() (alloc uint64, objects uint64) {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc, stats.HeapObjects
}

func TestSoakLeak(t *testing.T) {
	host := installFakeHost(t)
	setupTestPlugin(t)
	host.addAccount("1", "cn-account", sampleCredentialJSON("cn", "cn1"))
	host.setUpstream(func(method, url, body string) fakeUpstreamResponse {
		if strings.Contains(url, "/v2/chat/completions") {
			return fakeUpstreamResponse{Status: http.StatusOK, Body: strings.Join([]string{
				`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"glm-5.2","choices":[{"index":0,"delta":{"role":"assistant","content":"你好"}}]}`,
				"",
				`data: {"id":"chatcmpl-1","choices":[{"index":0,"delta":{"content":"，世界"}}],"usage":{"prompt_tokens":5,"completion_tokens":2}}`,
				"",
				"data: [DONE]",
				"",
			}, "\n")}
		}
		return fakeUpstreamResponse{Status: http.StatusNotFound, Body: `{"code":404}`}
	})

	// 大请求体：宿主每一条 SSE 行都会带上两份完整请求（可达数 MB）。
	bigBody := []byte(`{"id":"c1","choices":[],"pad":"` + strings.Repeat("x", *soakBodyBytes) + `"}`)

	step := func(i int) {
		switch *soakWorkload {
		case "exec":
			callMethod(t, pluginabi.MethodExecutorExecute, app.ExecutorRPCRequest{
				ExecutorRequest: chatRequest("cn:glm-5.2"),
			})
		case "stream":
			callMethod(t, pluginabi.MethodExecutorExecuteStream, app.ExecutorRPCRequest{
				ExecutorRequest: chatRequest("cn:glm-5.2"),
				StreamID:        fmt.Sprintf("s%d", i%8),
			})
			if !waitFor(t, 3e9, func() bool { return len(host.closedStreams()) > 0 }) {
				t.Fatalf("stream %d never closed", i)
			}
		case "normalize":
			callMethod(t, pluginabi.MethodResponseNormalizeBefore, pluginapi.ResponseTransformRequest{
				FromFormat: "openai",
				ToFormat:   "claude",
				Model:      "glm-5.2",
				Stream:     true,
				Body:       bigBody,
			})
		case "models":
			callMethod(t, pluginabi.MethodModelForAuth, map[string]any{"auth_index": "1"})
		default:
			t.Fatalf("unknown workload %q", *soakWorkload)
		}
		host.ResetRecords()
	}

	// 预热：让一次性缓存（模型清单、凭证解析等）先落地，避免把它算成泄漏。
	for i := 0; i < 200; i++ {
		step(i)
	}

	baseAlloc, baseObjects := sampleHeap()
	t.Logf("workload=%s baseline: heap=%.1fMiB objects=%d", *soakWorkload,
		float64(baseAlloc)/(1<<20), baseObjects)

	prevAlloc := baseAlloc
	for i := 1; i <= *soakIterations; i++ {
		step(i)
		if i%*soakEvery != 0 {
			continue
		}
		alloc, objects := sampleHeap()
		growth := int64(alloc) - int64(prevAlloc)
		t.Logf("iter=%6d heap=%.1fMiB objects=%d delta=%+.1fKiB (%+d obj)",
			i, float64(alloc)/(1<<20), objects, float64(growth)/1024, int64(objects)-int64(baseObjects))
		prevAlloc = alloc
	}

	finalAlloc, finalObjects := sampleHeap()
	total := int64(finalAlloc) - int64(baseAlloc)
	perIter := float64(total) / float64(*soakIterations)
	t.Logf("RESULT[%s]: %d iters, heap %+.2fMiB total, %+.1f bytes/iter, objects %+d",
		*soakWorkload, *soakIterations, float64(total)/(1<<20), perIter,
		int64(finalObjects)-int64(baseObjects))
	if perIter > 4096 {
		t.Fatalf("suspected leak: %.1f bytes retained per iteration", perIter)
	}
}

// chatRequest 构造一次最小的 chat completions 执行请求。
func chatRequest(model string) pluginapi.ExecutorRequest {
	return pluginapi.ExecutorRequest{
		AuthID:  "1",
		Model:   model,
		Format:  app.FormatChatCompletions,
		Payload: []byte(fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}]}`, model)),
	}
}

// 断言 ResponseTransformRequest 仍能被 JSON 编码（避免接口悄悄变化后压测失真）。
var _ = json.Marshal
