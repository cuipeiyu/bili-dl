package pipeline

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// mergeProgress 解析 ffmpeg -progress 的文本行，把播放时间戳换算成 0-100 的合并进度。
//
// ffmpeg 的 out_time_us（以及旧版 out_time_ms）单位实际都是微秒，典型输出：
//
//	frame=150
//	out_time_us=6000000
//	out_time=00:00:06.000000
//	progress=end
//
// 分母用任务的 Duration（playurl timelength，秒），是视频真实时长而非估算值。
type mergeProgress struct {
	durationSec int64 // 任务时长（秒），<=0 表示未知
	onProgress  func(pct float64)
	lastPct     float64
}

func newMergeProgress(durationSec int64, onProgress func(float64)) *mergeProgress {
	// lastPct 从 -1 起，确保首个 0% 能推送出去（进度条明确从 0 起步）。
	return &mergeProgress{durationSec: durationSec, onProgress: onProgress, lastPct: -1}
}

// scan 逐行消费 ffmpeg -progress 输出，直到 EOF 或读完。
// 在独立 goroutine 中调用，配合 cmd.StdoutPipe 使用。
func (m *mergeProgress) scan(r io.Reader) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "progress=end") {
			// 收尾帧强制 100，避免 faststart 二次封装期间停在 99。
			m.emit(100)
			continue
		}
		var micros int64
		if v, ok := parseKV(line, "out_time_us"); ok {
			micros = v
		} else if v, ok := parseKV(line, "out_time_ms"); ok {
			micros = v // 字段名是 ms，值实为微秒
		} else {
			continue
		}
		if m.durationSec <= 0 {
			continue // 时长未知，不显示假百分比
		}
		total := m.durationSec * 1_000_000
		if total <= 0 {
			continue
		}
		pct := float64(micros) / float64(total) * 100
		m.emit(pct)
	}
}

func (m *mergeProgress) emit(pct float64) {
	if pct < 0 {
		pct = 0
	} else if pct > 100 {
		pct = 100
	}
	// 同一百分比重复推送没有意义，节流掉。
	if pct == m.lastPct {
		return
	}
	m.lastPct = pct
	if m.onProgress != nil {
		m.onProgress(pct)
	}
}

// parseKV 解析 "key=value" 行，返回整型值。
func parseKV(line, key string) (int64, bool) {
	i := strings.IndexByte(line, '=')
	if i < 0 || line[:i] != key {
		return 0, false
	}
	v, err := strconv.ParseInt(strings.TrimSpace(line[i+1:]), 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
