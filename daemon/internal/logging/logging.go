// Package logging 集中配置 logrus：
// 文本格式带本地时间戳，级别可由 BILIDL_LOG_LEVEL 调整（debug/info/warn/error）。
package logging

import (
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Init 按级别字符串初始化全局 logger。未知级别回退 info。
func Init(level string) {
	// 日志时间用本地时区，方便对照 daemon 控制台与 popup 日志。
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
		ForceColors:     false,
	})
	log.SetOutput(os.Stdout)

	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug", "5":
		log.SetLevel(log.DebugLevel)
	case "warn", "warning", "3":
		log.SetLevel(log.WarnLevel)
	case "error", "2":
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}
}
