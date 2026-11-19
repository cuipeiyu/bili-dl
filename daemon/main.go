package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bili-dl/internal/api"
	"bili-dl/internal/config"
	"bili-dl/internal/deps"
	"bili-dl/internal/logging"
	"bili-dl/internal/pipeline"
	"bili-dl/internal/queue"
	"bili-dl/internal/store"

	log "github.com/sirupsen/logrus"
)

func main() {
	cfg := config.Load()
	logging.Init(cfg.LogLevel)

	if err := cfg.Validate(); err != nil {
		log.WithError(err).Fatal("配置错误")
	}
	if err := os.MkdirAll(cfg.OutputRoot, 0o755); err != nil {
		log.WithError(err).Fatal("创建输出目录失败")
	}

	// 启动自检：ffmpeg 不可用或输出目录不可写时直接拒绝启动，
	// 避免任务进了队列却在合并阶段才失败。
	ver, err := deps.FFmpeg(cfg.FFmpeg)
	if err != nil {
		log.WithError(err).Fatal("依赖检查失败")
	}
	log.WithField("ffmpeg", ver).Info("依赖检查通过：ffmpeg 可用")
	if err := deps.OutputRoot(cfg.OutputRoot); err != nil {
		log.WithError(err).Fatal("依赖检查失败：输出目录不可写")
	}
	log.WithField("output_root", cfg.OutputRoot).Info("依赖检查通过：输出目录可写")

	st, err := store.New(cfg.StateFile, nil)
	if err != nil {
		log.WithError(err).Fatal("加载任务状态失败")
	}
	defer st.Close()

	pl := pipeline.New(cfg)
	defer pl.Close()
	q := queue.New(pl, st)

	srv := api.New(cfg, st, q, pl, api.Diag{FFmpeg: ver, OutputRoot: cfg.OutputRoot})
	httpServer := &http.Server{
		Addr:              cfg.Addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.WithFields(log.Fields{
			"addr":          cfg.Addr,
			"output_root":   cfg.OutputRoot,
			"merge_workers": cfg.MergeWorkers,
			"download_tries": cfg.DownloadTries,
			"log_level":     cfg.LogLevel,
		}).Info("bili-dl daemon 已启动")
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.WithError(err).Fatal("服务启动失败")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("收到退出信号，优雅关闭...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.WithError(err).Warn("HTTP 关闭异常")
	}
	log.Info("daemon 已退出")
}
