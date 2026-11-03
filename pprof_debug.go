package main

// 诊断用：为插件的独立 Go runtime 暴露 pprof。
//
// 插件以 c-shared 形式加载，拥有自己的 runtime 和堆；宿主的
// /debug/pprof 看不到这里的分配。只有设置了 FREETIER_PPROF_ADDR
// （例如 127.0.0.1:8319）才会启动，未设置时零开销、不监听。
// 定位完成后可整文件删除。

import (
	"net/http"
	_ "net/http/pprof"
	"os"
)

func init() {
	addr := os.Getenv("FREETIER_PPROF_ADDR")
	if addr == "" {
		return
	}
	go func() {
		_ = http.ListenAndServe(addr, nil)
	}()
}
