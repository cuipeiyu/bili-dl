package pipeline

import (
	"math"
	"testing"
)

// 视频远大于音频时，总进度必须跟着视频走，不能被先下完的小音频虚抬。
func TestDownloadOverallByteWeighted(t *testing.T) {
	const span = 95.0
	const videoTotal, audioTotal = int64(4_800_000_000), int64(63_000_000)

	// 视频才下 10%、音频已下完：旧的平均算法会显示约 55%，加权后应约 10%。
	vw := int64(float64(videoTotal) * 0.10)
	pct := downloadOverall(vw, videoTotal, 10, audioTotal, audioTotal, 100, span)
	want := float64(vw+audioTotal) / float64(videoTotal+audioTotal) * span
	if math.Abs(pct-want) > 0.5 {
		t.Fatalf("加权进度 = %.2f, 期望 %.2f", pct, want)
	}
	if pct > 15 {
		t.Fatalf("小音频下完不应把总进度虚抬，实际 %.2f", pct)
	}

	// 两路都下完 → 恰好 95。
	pct = downloadOverall(videoTotal, videoTotal, 100, audioTotal, audioTotal, 100, span)
	if math.Abs(pct-span) > 1e-6 {
		t.Fatalf("全部完成应为 %.1f, 实际 %.2f", span, pct)
	}
}

func TestDownloadOverallUnknownTotalFallback(t *testing.T) {
	const span = 95.0
	// 任一路总量未知，退回两路百分比平均。
	pct := downloadOverall(0, 0, 60, 100, 200, 80, span)
	want := (60.0 + 80.0) / 2 / 100 * span
	if math.Abs(pct-want) > 1e-6 {
		t.Fatalf("回退进度 = %.2f, 期望 %.2f", pct, want)
	}
}

func TestDownloadOverallClamp(t *testing.T) {
	const span = 95.0
	if got := downloadOverall(200, 100, 200, 200, 100, 200, span); got != span {
		t.Fatalf("超出应钳制到 %.1f, 实际 %.2f", span, got)
	}
}
