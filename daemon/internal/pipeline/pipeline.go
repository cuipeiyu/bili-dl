// Package pipeline 实现下载 + 合并的真实逻辑。
// 下载：吞吐由调用方（queue）保证全局串行；单任务内 video/audio 两路并发。
// 合并：投递到独立 worker 池，并发数由 BILIDL_MERGE_WORKERS 控制。
package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"bili-dl/internal/config"
	"bili-dl/internal/model"

	log "github.com/sirupsen/logrus"
)

var invalidName = regexp.MustCompile(`[\\/:*?"<>|\x00-\x1f]`)

// byteLimiter 是简单的令牌桶，按字节限速，供两路下载共享以限制总带宽。
// 0 速率表示不限速。
type byteLimiter struct {
	mu     sync.Mutex
	rate   float64 // bytes per second
	tokens float64
	last   time.Time
}

func newByteLimiter(rateKBPS int) *byteLimiter {
	l := &byteLimiter{last: time.Now()}
	l.SetRate(rateKBPS)
	return l
}

// SetRate 运行时调整限速（KB/s），0 表示不限速。
func (l *byteLimiter) SetRate(rateKBPS int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if rateKBPS < 0 {
		rateKBPS = 0
	}
	l.rate = float64(rateKBPS) * 1024
	l.tokens = l.rate // 满桶起始，避免刚设置时限速过猛
	l.last = time.Now()
}

func (l *byteLimiter) RateKBPS() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int(l.rate / 1024)
}

// Wait 消耗 n 字节令牌，必要时阻塞；ctx 取消时立即返回 ctx.Err()。
// 用「预约」方式：把 tokens 扣成负数表示欠账，后续调用按时间累积补齐。
func (l *byteLimiter) Wait(ctx context.Context, n int64) error {
	l.mu.Lock()
	if l.rate <= 0 {
		l.mu.Unlock()
		return nil
	}
	now := time.Now()
	l.tokens += now.Sub(l.last).Seconds() * l.rate
	l.last = now
	capTokens := l.rate // 桶容量 = 1 秒速率
	if l.tokens > capTokens {
		l.tokens = capTokens
	}
	l.tokens -= float64(n) // 扣除本次用量，负数=需要等待补齐的欠账
	wait := time.Duration(0)
	if l.tokens < 0 {
		wait = time.Duration(-l.tokens / l.rate * float64(time.Second))
	}
	l.mu.Unlock()

	if wait <= 0 {
		return nil
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		// 取消时把刚才扣的令牌还回去，避免影响后续任务。
		l.mu.Lock()
		l.tokens += float64(n)
		l.mu.Unlock()
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Pipeline 持有下载客户端和合并 worker 池。
type Pipeline struct {
	cfg       config.Config
	client    *http.Client
	mergeJobs chan mergeJob
	stop      chan struct{}
	limiter   *byteLimiter // 两路下载共享的总带宽限速
}

type mergeJob struct {
	task      *model.Task
	videoPath string
	audioPath string
	update    func(*model.Task)
	ctx       context.Context
	done      func()
}

func New(cfg config.Config) *Pipeline {
	p := &Pipeline{
		cfg: cfg,
		// 不设 Client.Timeout：那是整个响应体的总时限，大文件会被误杀。
		// 卡死检测改在读取循环里用「空闲超时」判断（见 downloadOnce）。
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost:   4,
				IdleConnTimeout:       30 * time.Second,
				ResponseHeaderTimeout: 30 * time.Second, // 仅限制「等响应头」阶段
			},
		},
		mergeJobs: make(chan mergeJob, 128),
		stop:      make(chan struct{}),
		limiter:   newByteLimiter(cfg.SpeedLimitKBPS),
	}
	for i := 0; i < cfg.MergeWorkers; i++ {
		go p.mergeLoop()
	}
	return p
}

func (p *Pipeline) Close() { close(p.stop) }

// SetSpeedLimit 运行时调整下载总限速（KB/s，0=不限速）。
func (p *Pipeline) SetSpeedLimit(kbps int) {
	p.limiter.SetRate(kbps)
	log.WithField("limit_kbps", kbps).Info("下载限速已调整")
}

// SpeedLimit 返回当前限速（KB/s）。
func (p *Pipeline) SpeedLimit() int { return p.limiter.RateKBPS() }

// streamProgress 跟踪单条流的下载进度，各自独立，避免两路 total 互相覆盖。
type streamProgress struct {
	mu      sync.Mutex
	written int64
	total   int64
	pct     float64
	lastLog time.Time // 最近一次打进度日志的时间，用于节流
	// 速度采样：以最近 ~1 秒窗口内的字节增量计算，避免瞬时抖动。
	winStart     time.Time
	winBytes     int64
	speedBps     float64
	lastReported int64 // 上次上报的绝对字节数，用于算速度增量（续传重试不会重复计）
	finished     bool  // 该流已下载完成，速度固定为 0
}

// downloadOverall 按两路「已下载字节 / 总字节」加权算下载阶段总进度，结果落在 [0, span]。
// 体积小的音频完成得快，按字节加权后不会像百分比平均那样把总进度虚抬高。
// 任一路总长未知（<=0）时，该路退回用自身百分比折算成等价字节参与加权。
func downloadOverall(vw, vt int64, vpct float64, aw, at int64, apct float64, span float64) float64 {
	if vt > 0 && at > 0 {
		// 两路总量都已知：按字节加权，结果钳制到 [0, span]。
		pct := float64(vw+aw) / float64(vt+at) * span
		if pct > span {
			return span
		}
		if pct < 0 {
			return 0
		}
		return pct
	}
	// 任一路总量未知，无法可靠加权，退回两路百分比平均。
	pct := (vpct + apct) / 2 / 100 * span
	if pct > span {
		return span
	}
	if pct < 0 {
		return 0
	}
	return pct
}

// finish 在流下载完成后调用：保留进度与字节数，但清掉所有速度采样。
// 否则最后一次窗口的速度会一直残留，导致已完成的流仍显示速度。
func (sp *streamProgress) finish() {
	sp.mu.Lock()
	sp.finished = true
	sp.speedBps = 0
	sp.winBytes = 0
	sp.winStart = time.Time{}
	sp.mu.Unlock()
}

// report 接收「绝对已下载字节数」与总字节数。用绝对值而非增量，
// 是为了让断点续传/重试时进度起点正确、已下载部分不被重复累加。
func (sp *streamProgress) report(written, totalHint int64) {
	sp.mu.Lock()
	delta := written - sp.lastReported
	if delta < 0 {
		delta = 0
	}
	sp.lastReported = written
	sp.written = written
	sp.total = totalHint
	if sp.total > 0 {
		sp.pct = float64(sp.written) / float64(sp.total) * 100
		if sp.pct > 100 {
			sp.pct = 100
		}
	}
	// 速度窗口：满 1 秒结算一次，不足则累积。
	now := time.Now()
	sp.winBytes += delta
	if sp.winStart.IsZero() {
		sp.winStart = now
	} else if d := now.Sub(sp.winStart); d >= time.Second {
		sp.speedBps = float64(sp.winBytes) / d.Seconds()
		sp.winStart = now
		sp.winBytes = 0
	}
	sp.mu.Unlock()
}

func (sp *streamProgress) snapshot() (pct float64, written, total int64, speed string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.finished {
		return sp.pct, sp.written, sp.total, humanSpeed(0)
	}
	// 窗口未满 1 秒时，用已累积字节/已过时间给一个即时估算，避免速度长时间显示 0。
	bps := sp.speedBps
	if !sp.winStart.IsZero() {
		if d := time.Since(sp.winStart); d >= 200*time.Millisecond && sp.winBytes > 0 {
			bps = float64(sp.winBytes) / d.Seconds()
		}
	}
	return sp.pct, sp.written, sp.total, humanSpeed(bps)
}

// humanSpeed 把字节/秒转成人可读速度。
func humanSpeed(bps float64) string {
	if bps <= 0 {
		return "0 B/s"
	}
	const unit = 1024
	if bps < unit {
		return fmt.Sprintf("%.0f B/s", bps)
	}
	v := bps
	units := []string{"KB/s", "MB/s", "GB/s"}
	i := -1
	for v >= unit && i < len(units)-1 {
		v /= unit
		i++
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}

// humanSize 把字节数转成人可读字符串，用于日志和 popup 阶段说明。
func humanSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGTPE"[exp])
}

// Run 执行一次任务：下载两路流 → 投递合并。返回仅代表下载调度成功。
// ctx 被取消（暂停/移除）时立即中止下载与合并，清理临时文件，且不把任务标记为失败。
func (p *Pipeline) Run(ctx context.Context, t *model.Task, update func(*model.Task), done func()) error {
	L := log.WithField("task", t.ID)
	// 成功投递合并后，运行状态由 doMerge 结束时释放；其余退出路径这里释放。
	handedOff := false
	defer func() {
		if !handedOff && done != nil {
			done()
		}
	}()
	L.WithFields(log.Fields{
		"title": t.Title,
		"qn":    t.QN,
	}).Info("任务开始：准备目录并下载视频流 + 音频流（两路并发）")

	t.State = model.StateDownloading
	t.Stage = "准备下载"
	t.Progress = 0
	t.VideoPct = 0
	t.AudioPct = 0
	t.VideoSpeed = humanSpeed(0)
	t.AudioSpeed = humanSpeed(0)
	t.StageText = ""
	t.Error = ""
	update(t)

	tmpDir := filepath.Join(p.cfg.OutputRoot, ".tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return p.fail(t, update, err)
	}
	videoPath := filepath.Join(tmpDir, t.ID+"-v.part")
	audioPath := filepath.Join(tmpDir, t.ID+"-a.part")
	// 分片清理分三种情况：
	//   - 被暂停取消：保留，供继续下载断点续传；
	//   - 已投递合并：由 doMerge 在合并完成后清理（不能在这里删，worker 还要读）；
	//   - 其它（下载失败/daemon 关闭）：这里清理。
	defer func() {
		if ctx.Err() != nil || handedOff {
			return
		}
		os.Remove(videoPath)
		os.Remove(audioPath)
	}()

	vp := &streamProgress{lastLog: time.Now()}
	ap := &streamProgress{lastLog: time.Now()}

	// 总进度（Progress）单调映射到 0-100：下载阶段按两路字节数加权占 0-95%，
	// 合并阶段用 ffmpeg 真实进度占 95-100%。不再取两路百分比平均，避免
	// 体积很小的音频一下完就把总进度虚抬到 50% 以上；也不会在进入合并时回退到 0。
	// 各流单独百分比仍写 video_pct/audio_pct/merge_pct 供 popup 分条展示。
	// 每 128KB chunk 都会走到这里，直接 update 会让 WS 每秒广播上百次，
	// 把 popup 的 DOM 重建/日志渲染压垮。按时间节流到 ~5 帧/秒，肉眼仍平滑。
	const downloadSpan = 95.0
	var lastPush time.Time
	var lastStage string
	var lastOverall float64
	syncTask := func(stage string) {
		now := time.Now()
		// 阶段切换（如进入合并）必须立刻推，不受节流限制。
		if stage == lastStage && now.Sub(lastPush) < 200*time.Millisecond {
			return
		}
		lastPush = now
		lastStage = stage

		vpct, vw, vt, vspeed := vp.snapshot()
		apct, aw, at, aspeed := ap.snapshot()
		cp := *t
		cp.Stage = stage
		cp.VideoPct = vpct
		cp.AudioPct = apct
		cp.VideoSpeed = vspeed
		cp.AudioSpeed = aspeed
		if vt > 0 {
			cp.VideoSize = vt
		}
		if at > 0 {
			cp.AudioSize = at
		}
		overall := downloadOverall(vw, vt, vpct, aw, at, apct, downloadSpan)
		// 单调钳制：字节数只会增加，但重试/续传起点变化时也绝不回退。
		if overall < lastOverall {
			overall = lastOverall
		}
		lastOverall = overall
		cp.Progress = overall
		cp.StageText = fmt.Sprintf("视频 %s/%s @ %s · 音频 %s/%s @ %s",
			humanSize(vw), humanSize(vt), vspeed, humanSize(aw), humanSize(at), aspeed)
		update(&cp)
	}

	// 节流打进度：每 5 秒一条，避免刷屏。
	progressLog := func(label string, sp *streamProgress) {
		sp.mu.Lock()
		due := time.Since(sp.lastLog) >= 5*time.Second
		if due {
			sp.lastLog = time.Now()
		}
		pct, written, total := sp.pct, sp.written, sp.total
		sp.mu.Unlock()
		if due && total > 0 {
			L.WithFields(log.Fields{"stream": label, "pct": fmt.Sprintf("%.0f%%", pct),
				"bytes": humanSize(written) + "/" + humanSize(total), "speed": humanSpeed(sp.speedBps)}).Info("下载进度")
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	download := func(label, stageName, urlHost string, stream model.Stream, dest string, sp *streamProgress) {
		defer wg.Done()
		L.WithFields(log.Fields{"stream": label, "host": urlHost}).Info("开始下载流")
		cb := func(writtenAbs, totalHint int64) {
			sp.report(writtenAbs, totalHint)
			syncTask(stageName)
			progressLog(label, sp)
		}
		_, _, err := p.download(ctx, stream, dest, t.ID, label, cb)
		if err != nil {
			errCh <- fmt.Errorf("%s: %w", label, err)
			return
		}
		pct, written, total, _ := sp.snapshot()
		sp.finish() // 这条流已下完：速度立刻归零，不再残留最后的采样值
		L.WithFields(log.Fields{"stream": label, "bytes": humanSize(written),
			"total": humanSize(total), "pct": fmt.Sprintf("%.0f%%", pct)}).Info("流下载完成")
	}

	wg.Add(2)
	go download("视频流", "下载视频流", hostOf(t.Video.URL), t.Video, videoPath, vp)
	go download("音频流", "下载音频流", hostOf(t.Audio.URL), t.Audio, audioPath, ap)
	wg.Wait()
	close(errCh)

	// 两路真实字节数写回任务本体（syncTask 改的是副本，合并阶段拿的是 t）。
	// 下载中途也可能已由 syncTask 推过，这里以最终落盘字节为准覆盖。
	if _, vw, vt, _ := vp.snapshot(); vt > 0 {
		t.VideoSize = vt
	} else if vw > 0 {
		t.VideoSize = vw
	}
	if _, aw, at, _ := ap.snapshot(); at > 0 {
		t.AudioSize = at
	} else if aw > 0 {
		t.AudioSize = aw
	}

	// 被取消：保留 .part 供继续下载时断点续传，不清理、不标记失败。
	if ctx.Err() != nil {
		L.Info("下载被取消，保留已下载分片")
		return ctx.Err()
	}
	for err := range errCh {
		return p.fail(t, update, err)
	}

	// 合并前最后一道校验：两路分片都必须是有效媒体，杜绝错误页/空文件进入 ffmpeg。
	for _, part := range []struct{ path, label string }{
		{videoPath, "视频流"}, {audioPath, "音频流"},
	} {
		if verr := validateDownloadedFile(part.path); verr != nil {
			return p.fail(t, update, fmt.Errorf("%s分片无效：%w", part.label, verr))
		}
	}

	t.State = model.StateMerging
	t.Stage = "等待合并"
	t.Progress = 95
	syncTask("等待合并")
	L.Info("两路下载完成，已投递合并队列")

	select {
	case p.mergeJobs <- mergeJob{task: t, videoPath: videoPath, audioPath: audioPath, update: update, ctx: ctx, done: done}:
		handedOff = true
		return nil
	case <-ctx.Done():
		// 在等待合并期间被取消：.part 的 defer 清理会在下面按取消语义处理。
		L.Info("等待合并期间被取消，保留分片")
		return ctx.Err()
	case <-p.stop:
		return fmt.Errorf("daemon 关闭中")
	}
}

// hostOf 从 URL 取 host，只用于日志，不暴露完整签名 URL。
func hostOf(rawURL string) string {
	// 避免引入 net/url 的额外处理，简单截取 scheme:// 后到第一个 / 或 ? 的部分。
	s := rawURL
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	if i := strings.IndexAny(s, "/?"); i >= 0 {
		s = s[:i]
	}
	return s
}

func (p *Pipeline) download(ctx context.Context, stream model.Stream, dest, taskID, label string, report func(int64, int64)) (int64, int64, error) {
	var lastErr error
	for attempt := 1; attempt <= p.cfg.DownloadTries; attempt++ {
		if ctx.Err() != nil {
			return 0, 0, ctx.Err()
		}
		// 每次尝试都从磁盘 .part 现状继续，不再从头覆盖。
		have := existingSize(dest)
		if have > 0 {
			log.WithFields(log.Fields{"task": taskID, "stream": label, "resume_bytes": have}).
				Info("断点续传：从已下载位置继续")
		}
		n, total, err := p.downloadOnce(ctx, stream, dest, report)
		if err == nil {
			return n, total, nil
		}
		if ctx.Err() != nil {
			return n, total, ctx.Err()
		}
		lastErr = err
		// 403/410 视为直链过期，重试无意义，交给扩展重新提取。
		if strings.Contains(err.Error(), "status=403") || strings.Contains(err.Error(), "status=410") {
			return 0, 0, fmt.Errorf("直链已过期，请重新提取: %w", err)
		}
		if attempt < p.cfg.DownloadTries {
			backoff := time.Duration(attempt) * time.Second
			log.WithFields(log.Fields{"task": taskID, "stream": label, "attempt": attempt}).
				Warnf("下载失败，%v 后从已下载部分续传: %v", backoff, err)
			time.Sleep(backoff)
		}
	}
	return 0, 0, lastErr
}

// parseContentRange 解析 "bytes N-M/T"，返回起点、终点、总大小。
func parseContentRange(cr string) (start, end, total int64, ok bool) {
	cr = strings.TrimSpace(cr)
	if !strings.HasPrefix(strings.ToLower(cr), "bytes ") {
		return 0, 0, 0, false
	}
	spec := strings.TrimSpace(cr[len("bytes "):])
	slash := strings.LastIndexByte(spec, '/')
	if slash < 0 {
		return 0, 0, 0, false
	}
	rangePart := spec[:slash]
	sizePart := spec[slash+1:]
	dash := strings.IndexByte(rangePart, '-')
	if dash < 0 {
		return 0, 0, 0, false
	}
	var err error
	if start, err = strconv.ParseInt(strings.TrimSpace(rangePart[:dash]), 10, 64); err != nil || start < 0 {
		return 0, 0, 0, false
	}
	if end, err = strconv.ParseInt(strings.TrimSpace(rangePart[dash+1:]), 10, 64); err != nil || end < start {
		return 0, 0, 0, false
	}
	if sizePart != "*" {
		if total, err = strconv.ParseInt(strings.TrimSpace(sizePart), 10, 64); err != nil || total <= 0 {
			return 0, 0, 0, false
		}
	}
	return start, end, total, true
}

// existingSize 返回 .part 文件当前大小；不存在返回 0。
func existingSize(dest string) int64 {
	if fi, err := os.Stat(dest); err == nil {
		return fi.Size()
	}
	return 0
}

// badContentType 判断响应体是否为错误页而非媒体数据。
// B 站 CDN 在鉴权失败/直链过期时可能返回 200 + HTML/JSON，绝不能写进分片。
func badContentType(ct string) bool {
	ct = strings.ToLower(strings.TrimSpace(ct))
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	if strings.HasPrefix(ct, "text/") {
		return true
	}
	return ct == "application/json" || ct == "application/xml"
}

// looksLikeErrorBody 检查分片开头是否是 HTML/JSON/XML 错误页。
// fMP4 前几个字节是 box 长度 + "ftyp"，FLV 是 "FLV"，都不是这些字符。
func looksLikeErrorBody(head []byte) bool {
	s := strings.TrimSpace(string(head))
	if s == "" {
		return true
	}
	return s[0] == '<' || s[0] == '{' || s[0] == '['
}

// validateDownloadedFile 在下载完成后校验分片可用，避免把错误页或空文件送去合并。
func validateDownloadedFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("分片不可读: %w", err)
	}
	defer f.Close()
	head := make([]byte, 512)
	n, err := f.Read(head)
	if err != nil && err != io.EOF {
		return fmt.Errorf("分片读取失败: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("分片为空")
	}
	if looksLikeErrorBody(head[:n]) {
		return fmt.Errorf("分片内容是错误响应（HTML/JSON），直链可能已过期或防盗链失败")
	}
	return nil
}

// downloadOnce 执行一次 HTTP 下载，支持 Range 断点续传与空闲超时：
//   - 若 dest 已有 N 字节且服务端支持 Range，则请求 bytes=N- 并以追加方式续写；
//   - 若服务端忽略 Range 返回 200，则从头覆盖；
//   - 空闲超时按「连续 N 秒未收到任何数据」判定，而非整个下载的总时限。
func (p *Pipeline) downloadOnce(ctx context.Context, stream model.Stream, dest string, report func(int64, int64)) (int64, int64, error) {
	have := existingSize(dest)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, stream.URL, nil)
	if err != nil {
		return 0, 0, err
	}
	for k, v := range stream.Headers {
		if isHopByHop(k) {
			continue
		}
		req.Header.Set(k, v)
	}
	// Range 必须由本端按 .part 实际大小生成，绝不能用抓包带来的值。
	// 播放器抓到的是它自己的分片范围（如 bytes=2519549-2626807），
	// 原样发出会让服务端返回中间片段，却被当作从 0 开始写入，文件整体错位。
	// 即使 have=0 也要 Del，避免首次下载沿用抓包 Range。
	req.Header.Del("Range")
	if have > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(have, 10)+"-")
	}

	// 派生子 ctx：外部暂停/移除可取消，空闲看门狗也能 cancel；二者独立不互相覆盖。
	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	req = req.WithContext(reqCtx)

	resp, err := p.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return have, 0, ctx.Err()
		}
		return have, 0, err
	}
	defer resp.Body.Close()

	// 判定续传结果：206 表示服务端接受 Range；200 表示从头；416 表示范围越界（可能已下完）。
	var base int64
	var total int64
	appendMode := false
	switch resp.StatusCode {
	case http.StatusPartialContent: // 206
		base = have
		appendMode = true
		// Content-Range: bytes N-M/T，T 是完整大小，N 是本次数据起点。
		// 必须校验 N==have：若服务端忽略/改写 Range，返回的是别的偏移片段，
		// 照常追加会造成文件错位且总字节数碰巧对得上，最终送进 ffmpeg 才暴露为 Invalid data。
		total = have + resp.ContentLength
		if cr := resp.Header.Get("Content-Range"); cr != "" {
			start, end, size, ok := parseContentRange(cr)
			if ok {
				if start != have {
					return have, 0, fmt.Errorf("Range 响应起点不符：请求 %d，返回 %d（%s）", have, start, cr)
				}
				if size > 0 {
					total = size
				} else if end >= start {
					total = end + 1
				}
			}
		}
	case http.StatusOK: // 200：服务端忽略 Range，从头写
		base = 0
		total = resp.ContentLength
	case http.StatusRequestedRangeNotSatisfiable: // 416：请求范围不满足
		// 只有分片确实已完整（have>0 且内容有效）才算续传完成。
		// 直接从零字节走 416 会返回成功但根本没有文件，合并必然失败。
		if have > 0 {
			if verr := validateDownloadedFile(dest); verr == nil {
				return have, have, nil
			}
			// 分片损坏：删掉从零重下，下面按 200 覆盖处理。
			os.Remove(dest)
			have = 0
			req.Header.Del("Range")
			log.WithFields(log.Fields{"stream": labelOf(dest), "reason": "416 但分片损坏"}).Warn("丢弃损坏分片，重新下载")
			return p.downloadOnce(ctx, stream, dest, report)
		}
		return have, 0, fmt.Errorf("status=416 且无已下载分片")
	default:
		return have, 0, fmt.Errorf("status=%d", resp.StatusCode)
	}

	// 内容类型检查：防止把 CDN 返回的 HTML/JSON 错误页当媒体写入分片。
	if badContentType(resp.Header.Get("Content-Type")) {
		return have, 0, fmt.Errorf("响应不是媒体数据（Content-Type=%s），直链可能已过期", resp.Header.Get("Content-Type"))
	}

	// 磁盘空间预检：输出目录至少要有 2 倍体积可用（分片 1 份 + 合并临时产物 1 份）。
	// 续传时已落盘的 base 属于第一份的一部分，从需求里扣掉，避免误报。
	if need := total*diskReserveFactor - base; need > 0 {
		if err := ensureDiskSpace(filepath.Dir(dest), need, labelOf(dest)+"下载"); err != nil {
			return base, total, err
		}
	}

	flags := os.O_WRONLY | os.O_CREATE
	if appendMode {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(dest, flags, 0o644)
	if err != nil {
		return base, total, err
	}
	defer f.Close()

	// 上报起点：续传时进度条从 base 开始，不因重试重复累加。
	report(base, total)

	// 空闲超时：读循环每次收到数据就刷新 lastRead；看门狗每秒检查，
	// 超过空闲阈值就 cancel 请求上下文，强制中断阻塞的 Read。
	var lastRead = time.Now()
	var muLast sync.Mutex
	idle := p.cfg.HTTPIdleTimeout
	watchdogDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-watchdogDone:
				return
			case <-reqCtx.Done():
				return
			case <-ticker.C:
				muLast.Lock()
				stale := time.Since(lastRead)
				muLast.Unlock()
				if stale > idle {
					log.WithFields(log.Fields{"stream": labelOf(dest), "idle": stale.Round(time.Second).String()}).
						Warn("下载空闲超时，中断本次请求以触发续传")
					cancel()
					return
				}
			}
		}
	}()
	defer close(watchdogDone)

	written := base
	buf := make([]byte, 128<<10)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			// 限速：两路共享令牌桶，等待期间可被暂停/移除打断。
			if err := p.limiter.Wait(reqCtx, int64(n)); err != nil {
				return written, total, err
			}
			w, werr := f.Write(buf[:n])
			written += int64(w)
			if werr != nil {
				return written, total, werr
			}
			muLast.Lock()
			lastRead = time.Now()
			muLast.Unlock()
			report(written, total)
		}
		if rerr == io.EOF {
			// 字节数不足说明连接被提前截断，必须当失败触发续传，
			// 否则不完整的分片会被当成功送进合并，ffmpeg 报 Invalid data。
			if total > 0 && written < total {
				return written, total, fmt.Errorf("下载不完整：%d/%d 字节，连接被提前关闭", written, total)
			}
			break
		}
		if rerr != nil {
			// 外部暂停/移除：直接把 ctx 错误上抛，上层据此跳过失败标记。
			if ctx.Err() != nil {
				return written, total, ctx.Err()
			}
			if reqCtx.Err() == context.Canceled {
				return written, total, fmt.Errorf("下载空闲超时（%v 无数据）", idle)
			}
			return written, total, rerr
		}
	}
	return written, total, nil
}

// labelOf 从 .part 文件名粗略推断是视频还是音频，仅用于日志。
func labelOf(dest string) string {
	if strings.HasSuffix(dest, "-v.part") {
		return "视频流"
	}
	if strings.HasSuffix(dest, "-a.part") {
		return "音频流"
	}
	return "下载流"
}

func (p *Pipeline) mergeLoop() {
	for {
		select {
		case job := <-p.mergeJobs:
			p.doMerge(job)
		case <-p.stop:
			return
		}
	}
}

// zeroSpeeds 把任务上的流速度归零，用于合并/失败等非下载中状态。
func zeroSpeeds(t *model.Task) {
	t.VideoSpeed = humanSpeed(0)
	t.AudioSpeed = humanSpeed(0)
}

func (p *Pipeline) doMerge(job mergeJob) {
	t := job.task
	zeroSpeeds(t) // 进入合并即不再下载，速度固定为 0
	L := log.WithField("task", t.ID)
	if job.done != nil {
		defer job.done()
	}
	outName, pathErr := p.outputName(t)
	if pathErr != nil {
		cp := *t
		cp.State = model.StateFailed
		cp.Stage = "路径错误"
		cp.Error = "保存路径非法: " + pathErr.Error()
		job.update(&cp)
		L.WithError(pathErr).Error("保存路径非法")
		return
	}
	outPath := filepath.Join(p.cfg.OutputRoot, filepath.FromSlash(outName))
	// 用户可指定子目录，合并前先建好，避免 ffmpeg 写 tmp 时失败。
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		cp := *t
		cp.State = model.StateFailed
		cp.Stage = "创建目录失败"
		cp.Error = "创建输出目录失败: " + err.Error()
		job.update(&cp)
		L.WithError(err).Error("创建输出目录失败")
		return
	}
	tmpOut := outPath + ".tmp"

	// 合并前精确校验：分片已在盘上占了一份，tmpOut 还要再写一份同等体积。
	// 用分片实际大小（视频 + 音频）而非预估，避免下载阶段单路粗筛的误差漏网。
	partBytes := existingSize(job.videoPath) + existingSize(job.audioPath)
	if err := ensureDiskSpace(filepath.Dir(tmpOut), partBytes, "合并产物"); err != nil {
		cp := *t
		cp.State = model.StateFailed
		cp.Stage = "合并空间不足"
		cp.Error = err.Error()
		job.update(&cp)
		L.WithError(err).Error("合并前磁盘空间检查失败")
		return
	}

	cp := *t
	cp.Stage = "合并中"
	cp.Progress = 95 // 下载已完成 95%，合并阶段从 95 起，绝不回退
	cp.MergePct = 0
	cp.StageText = "ffmpeg 正在封装 MP4"
	job.update(&cp)
	L.WithFields(log.Fields{"ffmpeg": p.cfg.FFmpeg, "output": outName}).Info("开始合并（ffmpeg -c copy）")

	jobCtx := job.ctx
	if jobCtx == nil {
		jobCtx = context.Background()
	}
	cmd := exec.CommandContext(jobCtx, p.cfg.FFmpeg,
		"-y", "-hide_banner", "-loglevel", "error",
		"-nostats", "-progress", "pipe:1",
		"-i", job.videoPath, "-i", job.audioPath,
		"-map", "0:v:0", "-map", "1:a:0",
		"-c", "copy", "-movflags", "+faststart",
		// tmpOut 后缀是 .tmp，ffmpeg 无法据此推断封装格式，必须显式指定。
		"-f", "mp4",
		tmpOut,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		os.Remove(tmpOut)
		cp := *t
		cp.State = model.StateFailed
		cp.Stage = "合并失败"
		cp.Error = "建立进度管道失败: " + err.Error()
		job.update(&cp)
		L.WithError(err).Error("建立进度管道失败")
		return
	}

	// 进度回调在独立 goroutine 里触发：构造独立副本推送，不共享循环外的 cp。
	merged := newMergeProgress(t.Duration, func(pct float64) {
		if pct < 0 {
			pct = 0
		} else if pct > 100 {
			pct = 100
		}
		pc := *t
		pc.State = model.StateMerging
		pc.Stage = "合并中"
		pc.MergePct = pct           // 真实合并百分比，供 popup 合并子行显示
		pc.Progress = 95 + pct*0.05 // 总进度尾段 95→100
		pc.StageText = "ffmpeg 正在封装 MP4"
		job.update(&pc)
	})

	start := time.Now()
	if err := cmd.Start(); err != nil {
		os.Remove(tmpOut)
		cp := *t
		cp.State = model.StateFailed
		cp.Stage = "合并失败"
		cp.Error = "ffmpeg 启动失败: " + err.Error()
		job.update(&cp)
		L.WithError(err).Error("ffmpeg 启动失败")
		return
	}
	// 必须在 Wait 前把 stdout 读完，否则管道写满会阻塞 ffmpeg。
	progressDone := make(chan struct{})
	go func() {
		merged.scan(stdout)
		close(progressDone)
	}()
	waitErr := cmd.Wait()
	<-progressDone

	if waitErr != nil {
		os.Remove(tmpOut)
		if jobCtx.Err() != nil {
			L.Info("合并被取消，跳过失败标记")
			return
		}
		cp := *t
		cp.State = model.StateFailed
		cp.Stage = "合并失败"
		cp.Error = "ffmpeg 合并失败: " + strings.TrimSpace(stderr.String()) + " " + waitErr.Error()
		job.update(&cp)
		L.WithFields(log.Fields{
			"stderr": strings.TrimSpace(stderr.String()),
		}).WithError(waitErr).Error("合并失败")
		return
	}
	if err := os.Rename(tmpOut, outPath); err != nil {
		os.Remove(tmpOut)
		cp := *t
		cp.State = model.StateFailed
		cp.Stage = "落盘失败"
		cp.Error = "落盘失败: " + err.Error()
		job.update(&cp)
		L.WithError(err).Error("合并产物落盘失败")
		return
	}
	cp = *t
	cp.State = model.StateDone
	cp.Stage = "完成"
	cp.StageText = ""
	cp.Progress = 100
	cp.MergePct = 100
	cp.Output = outName
	job.update(&cp)

	// 合并落定后清理分片（取消场景上面已提前 return，保留供续传）。
	os.Remove(job.videoPath)
	os.Remove(job.audioPath)

	fi, _ := os.Stat(outPath)
	size := ""
	if fi != nil {
		size = humanSize(fi.Size())
	}
	L.WithFields(log.Fields{"output": outName, "size": size, "elapsed": time.Since(start).Round(time.Millisecond).String()}).
		Info("任务完成，文件已落盘")
}

func (p *Pipeline) fail(t *model.Task, update func(*model.Task), err error) error {
	zeroSpeeds(t)
	cp := *t
	cp.State = model.StateFailed
	cp.Stage = "下载失败"
	cp.Error = err.Error()
	update(&cp)
	log.WithField("task", t.ID).WithError(err).Error("任务失败")
	return err
}

func sanitizeName(title, bvid string, qn int) string {
	title = invalidName.ReplaceAllString(title, "_")
	title = strings.TrimSpace(title)
	if len(title) > 80 {
		title = title[:80]
	}
	if title == "" {
		title = "video"
	}
	return fmt.Sprintf("%s-%s-%d.mp4", title, bvid, qn)
}

func (p *Pipeline) outputName(t *model.Task) (string, error) {
	if strings.TrimSpace(t.OutputPath) != "" {
		return NormalizeOutputPath(t.OutputPath)
	}
	return sanitizeName(t.Title, t.BVID, t.QN), nil
}

// NormalizeOutputPath 把用户填写的保存路径规范为 OutputRoot 下的安全相对路径。
// 允许 / 或 \ 分隔子目录；绝对路径前缀与 .. 段直接丢弃，避免逃出输出根目录；
// 非法文件名字符替换为 _，没有 .mp4 后缀时补齐。
func NormalizeOutputPath(p string) (string, error) {
	p = strings.TrimSpace(strings.ReplaceAll(p, "\\", "/"))
	if p == "" {
		return "", fmt.Errorf("output_path 为空")
	}
	parts := strings.Split(p, "/")
	clean := make([]string, 0, len(parts))
	for _, seg := range parts {
		seg = strings.TrimSpace(seg)
		if seg == "" || seg == "." || seg == ".." {
			continue
		}
		// 冒号（Windows 盘符/ADS）替换为下划线，不引入绝对路径语义。
		seg = strings.ReplaceAll(seg, ":", "_")
		seg = invalidName.ReplaceAllString(seg, "_")
		seg = strings.Trim(seg, " ")
		if r := []rune(seg); len(r) > 120 {
			seg = string(r[:120])
		}
		if seg != "" {
			clean = append(clean, seg)
		}
	}
	if len(clean) == 0 {
		return "", fmt.Errorf("output_path 无有效路径段")
	}
	out := strings.Join(clean, "/")
	if !strings.HasSuffix(strings.ToLower(out), ".mp4") {
		out += ".mp4"
	}
	if len(out) > 500 {
		return "", fmt.Errorf("output_path 过长")
	}
	return out, nil
}

func isHopByHop(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
		"te", "trailer", "transfer-encoding", "upgrade", "host",
		"content-length", "accept-encoding":
		return true
	}
	return false
}
