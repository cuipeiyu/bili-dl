package pipeline

import (
	"fmt"
	"syscall"

	log "github.com/sirupsen/logrus"
)

// diskReserveFactor 是下载前预留的倍数：
// 分片占 1 份，合并时 tmpOut 与分片并存再占 1 份，合计 2 份。
const diskReserveFactor = 2

// freeDiskBytes 返回 dir 所在文件系统的可用字节数。
// 用 Bavail（普通用户可用块）而非 Bfree，避免把 root 保留块算进来误判为够用。
func freeDiskBytes(dir string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}

// ensureDiskSpace 校验 dir 所在文件系统的可用空间不小于 need。
// need <= 0 时（如 Content-Length 未知）跳过检查，交给合并前的校验兜底。
func ensureDiskSpace(dir string, need int64, what string) error {
	if need <= 0 {
		return nil
	}
	free, err := freeDiskBytes(dir)
	if err != nil {
		// 检查本身失败（特殊挂载不支持 Statfs 等）不应阻断下载，告警放行。
		log.WithError(err).WithField("dir", dir).Warn("无法读取磁盘可用空间，跳过空间预检")
		return nil
	}
	if free < need {
		return fmt.Errorf("磁盘空间不足（%s）：可用 %s，至少需要 %s",
			what, humanSize(free), humanSize(need))
	}
	return nil
}
