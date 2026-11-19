package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config 全部通过环境变量配置，便于 Docker 部署。
type Config struct {
	Addr          string
	Token         string
	OutputRoot    string
	StateFile     string
	FFmpeg        string
	MergeWorkers  int
	DownloadTries int
	// HTTPIdleTimeout 是「连续多久收不到任何数据」判定卡死的空闲超时，
	// 不是整个下载的总时限——大文件下载时间可能很长，用总时限会误杀。
	HTTPIdleTimeout time.Duration
	LogLevel        string
	// SpeedLimitKBPS 是下载总带宽上限（KB/s），0 表示不限速；运行时可通过 API 调整。
	SpeedLimitKBPS int
}

// Load 读取配置，缺失项使用默认值。
func Load() Config {
	return Config{
		Addr:          env("BILIDL_ADDR", ":18080"),
		Token:         os.Getenv("BILIDL_TOKEN"),
		OutputRoot:    env("BILIDL_OUTPUT_ROOT", "./data"),
		StateFile:     env("BILIDL_STATE_FILE", "./data/tasks.json"),
		FFmpeg:        env("BILIDL_FFMPEG", "ffmpeg"),
		MergeWorkers:  envInt("BILIDL_MERGE_WORKERS", 2),
		DownloadTries: envInt("BILIDL_DOWNLOAD_TRIES", 3),
		HTTPIdleTimeout: time.Duration(envInt("BILIDL_HTTP_IDLE_TIMEOUT_SEC", envInt("BILIDL_HTTP_TIMEOUT_SEC", 30))) * time.Second,
		LogLevel:        env("BILIDL_LOG_LEVEL", "info"),
		SpeedLimitKBPS:  envInt("BILIDL_SPEED_LIMIT_KBPS", 0),
	}
}

// Validate 检查必填项。Token 为空时拒绝启动，避免服务裸奔。
func (c Config) Validate() error {
	if c.Token == "" {
		return fmt.Errorf("BILIDL_TOKEN 不能为空")
	}
	if c.MergeWorkers < 1 {
		return fmt.Errorf("BILIDL_MERGE_WORKERS 必须 >= 1")
	}
	if c.DownloadTries < 1 {
		return fmt.Errorf("BILIDL_DOWNLOAD_TRIES 必须 >= 1")
	}
	if c.HTTPIdleTimeout < 1 {
		return fmt.Errorf("BILIDL_HTTP_IDLE_TIMEOUT_SEC 必须 >= 1")
	}
	if c.SpeedLimitKBPS < 0 {
		return fmt.Errorf("BILIDL_SPEED_LIMIT_KBPS 不能为负数")
	}
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
