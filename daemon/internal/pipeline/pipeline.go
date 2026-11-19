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
	mu      sync.Mutex
	rate    float64 // bytes per second
	tokens  float64
	last    time.Time
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
				MaxIdleConnsPerHost: 4,
				IdleConnTimeout:     30 * time.Second,
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
	mu       sync.Mutex
	written  int64
	total    int64
	pct      float64
	lastLog  time.Time // 最近一次打进度日志的时间，用于节流
	// 速度采样：以最近 ~1 秒窗口内的字节增量计算，避免瞬时抖动。
	winStart time.Time
	winBytes int64
	speedBps float64
	lastReported int64 // 上次上报的绝对字节数，用于算速度增量（续传重试不会重复计）
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

	// 合并两路进度到任务：总进度取两路平均，各流单独字段供 popup 分条展示。
	syncTask := func(stage string) {
		vpct, vw, vt, vspeed := vp.snapshot()
		apct, aw, at, aspeed := ap.snapshot()
		cp := *t
		cp.Stage = stage
		cp.VideoPct = vpct
		cp.AudioPct = apct
		cp.VideoSpeed = vspeed
		cp.AudioSpeed = aspeed
		cp.Progress = (vpct + apct) / 2
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
		L.WithFields(log.Fields{"stream": label, "bytes": humanSize(written),
			"total": humanSize(total), "pct": fmt.Sprintf("%.0f%%", pct)}).Info("流下载完成")
	}

	wg.Add(2)
	go download("视频流", "下载视频流", hostOf(t.Video.URL), t.Video, videoPath, vp)
	go download("音频流", "下载音频流", hostOf(t.Audio.URL), t.Audio, audioPath, ap)
	wg.Wait()
	close(errCh)

	// 被取消：保留 .part 供继续下载时断点续传，不清理、不标记失败。
	if ctx.Err() != nil {
		L.Info("下载被取消，保留已下载分片")
		return ctx.Err()
	}
	for err := range errCh {
		return p.fail(t, update, err)
	}

	t.State = model.StateMerging
	t.Stage = "等待合并"
	t.Progress = 99
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

// existingSize 返回 .part 文件当前大小；不存在返回 0。
func existingSize(dest string) int64 {
	if fi, err := os.Stat(dest); err == nil {
		return fi.Size()
	}
	return 0
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
		// Content-Range: bytes N-M/T 里的 T 是完整大小；退回用 N + 本次 ContentLength。
		total = have + resp.ContentLength
		if cr := resp.Header.Get("Content-Range"); cr != "" {
			if slash := strings.LastIndex(cr, "/"); slash >= 0 {
				if t, e := strconv.ParseInt(strings.TrimSpace(cr[slash+1:]), 10, 64); e == nil && t > 0 {
					total = t
				}
			}
		}
	case http.StatusOK: // 200：服务端忽略 Range，从头写
		base = 0
		total = resp.ContentLength
	case http.StatusRequestedRangeNotSatisfiable: // 416：请求范围不满足
		return have, have, nil
	default:
		return have, 0, fmt.Errorf("status=%d", resp.StatusCode)
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

func (p *Pipeline) doMerge(job mergeJob) {
	t := job.task
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

	cp := *t
	cp.Stage = "合并中"
	cp.Progress = 99
	cp.StageText = "ffmpeg 正在封装 MP4"
	job.update(&cp)
	L.WithFields(log.Fields{"ffmpeg": p.cfg.FFmpeg, "output": outName}).Info("开始合并（ffmpeg -c copy）")

	jobCtx := job.ctx
	if jobCtx == nil {
		jobCtx = context.Background()
	}
	cmd := exec.CommandContext(jobCtx, p.cfg.FFmpeg,
		"-y", "-hide_banner", "-loglevel", "error",
		"-i", job.videoPath, "-i", job.audioPath,
		"-map", "0:v:0", "-map", "1:a:0",
		"-c", "copy", "-movflags", "+faststart",
		// tmpOut 后缀是 .tmp，ffmpeg 无法据此推断封装格式，必须显式指定。
		"-f", "mp4",
		tmpOut,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	start := time.Now()
	if err := cmd.Run(); err != nil {
		os.Remove(tmpOut)
		if jobCtx.Err() != nil {
			L.Info("合并被取消，跳过失败标记")
			return
		}
		cp := *t
		cp.State = model.StateFailed
		cp.Stage = "合并失败"
		cp.Error = "ffmpeg 合并失败: " + strings.TrimSpace(stderr.String()) + " " + err.Error()
		job.update(&cp)
		L.WithError(err).Error("合并失败")
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
