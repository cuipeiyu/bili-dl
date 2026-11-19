// Package deps 在启动时检查外部依赖，避免任务下发后才发现 ffmpeg 不可用。
package deps

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// FFmpeg 检查 ffmpeg 可执行文件存在且能运行，返回版本字符串。
// bin 可以是绝对路径或 PATH 中的名字。
func FFmpeg(bin string) (string, error) {
	if strings.TrimSpace(bin) == "" {
		return "", fmt.Errorf("ffmpeg 路径为空")
	}
	path, err := exec.LookPath(bin)
	if err != nil {
		return "", fmt.Errorf("找不到 ffmpeg (%s): %w", bin, err)
	}
	cmd := exec.Command(path, "-version")
	cmd.Stderr = nil
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("ffmpeg 无法执行 (%s): %w", path, err)
	}
	first := strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0])
	if first == "" {
		return "", fmt.Errorf("ffmpeg -version 无输出 (%s)", path)
	}
	return fmt.Sprintf("%s — %s", path, first), nil
}

// OutputRoot 检查输出目录存在、可写，并返回用于探测的临时文件路径。
// 调用方用完后删除该文件。
func OutputRoot(root string) error {
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("输出目录不可访问 (%s): %w", root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("输出路径不是目录 (%s)", root)
	}
	probe := filepath.Join(root, ".writetest-"+fmt.Sprint(time.Now().UnixNano()))
	f, err := os.Create(probe)
	if err != nil {
		return fmt.Errorf("输出目录不可写 (%s): %w", root, err)
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	return nil
}
