package pipeline

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"bili-dl/internal/config"
	"bili-dl/internal/model"
)

func TestParseContentRange(t *testing.T) {
	cases := []struct {
		in                string
		start, end, total int64
		ok                bool
	}{
		{"bytes 2519549-2626807/5204327861", 2519549, 2626807, 5204327861, true},
		{"bytes 0-127/5204327861", 0, 127, 5204327861, true},
		{"bytes 100-199/*", 100, 199, 0, true},
		{"bytes */5204327861", 0, 0, 0, false},
		{"garbage", 0, 0, 0, false},
	}
	for _, c := range cases {
		start, end, total, ok := parseContentRange(c.in)
		if ok != c.ok || start != c.start || end != c.end || total != c.total {
			t.Errorf("%q => (%d,%d,%d,%v), want (%d,%d,%d,%v)",
				c.in, start, end, total, ok, c.start, c.end, c.total, c.ok)
		}
	}
}

// 抓包头里残留的 Range 不能被沿用：首次下载必须从 0 开始，
// 否则服务端只返回中间分片，却被当作文件开头写入。
func TestDownloadIgnoresCapturedRange(t *testing.T) {
	const payload = "0123456789abcdefghijklmnopqrstuvwxyz"
	var gotRange []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRange = append(gotRange, r.Header.Get("Range"))
		// 模拟 CDN 严格执行抓包带来的 Range：只回中间一段。
		rng := r.Header.Get("Range")
		if rng != "" {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Range", "bytes 10-15/"+strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte(payload[10:16]))
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		_, _ = w.Write([]byte(payload))
	}))
	defer srv.Close()

	cfg := config.Config{DownloadTries: 1, HTTPIdleTimeout: 30 * 1e9, SpeedLimitKBPS: 0}
	p := New(cfg)
	dir := t.TempDir()
	dest := filepath.Join(dir, "v.part")

	// 抓包头带了一个无关的 Range。
	stream := model.Stream{
		URL:     srv.URL,
		Headers: map[string]string{"Range": "bytes=10-15", "Accept-Encoding": "identity"},
	}
	_, _, err := p.downloadOnce(context.Background(), stream, dest, func(int64, int64) {})
	if err != nil {
		t.Fatalf("downloadOnce: %v", err)
	}
	if len(gotRange) != 1 || gotRange[0] != "" {
		t.Fatalf("抓包 Range 未被清除：请求头发出了 %q", gotRange)
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != payload {
		t.Fatalf("分片内容错位：got %q, want %q", data, payload)
	}
}

// 续传时服务端返回错误起点必须报错，不能按本地 have 追加。
func TestDownloadRejectsWrongRangeStart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Range", "bytes 999-1004/2000")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("abcdef"))
	}))
	defer srv.Close()

	p := New(config.Config{DownloadTries: 1, HTTPIdleTimeout: 30 * 1e9})
	dir := t.TempDir()
	dest := filepath.Join(dir, "v.part")
	if err := os.WriteFile(dest, []byte("0123456789"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, err := p.downloadOnce(context.Background(), model.Stream{URL: srv.URL}, dest, func(int64, int64) {})
	if err == nil || !strings.Contains(err.Error(), "起点不符") {
		t.Fatalf("应拒绝错位 Range，实际 err=%v", err)
	}
}

// 已存在的抓包 Range 不会污染下一次任务（模拟 tasks.json 里的老任务）。
func TestLegacyCapturedRangeStillOverridden(t *testing.T) {
	const payload = "abcdefghijklmnopqrstuvwxyz"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := fmt.Sprintf("bytes=%d-", 0)
		if got := r.Header.Get("Range"); got != "" && got != want {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Range", "bytes 10-15/"+strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte(payload[10:16]))
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte(payload))
	}))
	defer srv.Close()

	p := New(config.Config{DownloadTries: 1, HTTPIdleTimeout: 30 * 1e9})
	dest := filepath.Join(t.TempDir(), "v.part")
	stream := model.Stream{URL: srv.URL, Headers: map[string]string{"Range": "bytes=2519549-2626807"}}
	_, _, err := p.download(context.Background(), stream, dest, "task", "视频流", func(int64, int64) {})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(dest)
	if string(data) != payload {
		t.Fatalf("老任务 Range 污染：got %q, want %q", data, payload)
	}
}
