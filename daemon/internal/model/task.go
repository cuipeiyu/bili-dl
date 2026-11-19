package model

import "time"

// TaskState 是任务生命周期状态。
type TaskState string

const (
	StatePending     TaskState = "pending"
	StateDownloading TaskState = "downloading"
	StatePaused      TaskState = "paused"
	StateMerging     TaskState = "merging"
	StateDone        TaskState = "done"
	StateFailed      TaskState = "failed"
	StateCanceled    TaskState = "canceled"
)

// Stream 描述一条媒体流及其下载所需的请求头。
// Headers 由扩展从浏览器真实请求中抓取，原样透传给下载器。
type Stream struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
}

// Task 是一次下载合并任务。
type Task struct {
	ID         string    `json:"id"`
	BVID       string    `json:"bvid,omitempty"`
	CID        int64     `json:"cid,omitempty"`
	QN         int       `json:"qn,omitempty"`
	Title      string    `json:"title,omitempty"`
	Duration   int64     `json:"duration,omitempty"`
	VideoSize  int64     `json:"video_size,omitempty"`  // 视频流真实字节数（下载时由 Content-Range 得到）
	AudioSize  int64     `json:"audio_size,omitempty"`  // 音频流真实字节数
	OutputPath string    `json:"output_path,omitempty"` // 用户指定的保存路径，相对 OutputRoot，可含子目录
	Video      Stream    `json:"video"`
	Audio      Stream    `json:"audio"`
	State      TaskState `json:"state"`
	Progress   float64   `json:"progress"`
	Stage      string    `json:"stage,omitempty"`       // 人类可读的当前阶段，如「下载视频流」「合并中」
	VideoPct   float64   `json:"video_pct,omitempty"`   // 视频流单独进度 0-100
	AudioPct   float64   `json:"audio_pct,omitempty"`   // 音频流单独进度 0-100
	MergePct   float64   `json:"merge_pct,omitempty"`   // 合并真实进度 0-100（总进度里合并只占 95-100 尾段）
	VideoSpeed string    `json:"video_speed,omitempty"` // 视频流当前速度，如「3.2 MB/s」
	AudioSpeed string    `json:"audio_speed,omitempty"` // 音频流当前速度
	StageText  string    `json:"stage_text,omitempty"`  // 阶段补充说明，如「12.3 / 45.6 MB」
	Error      string    `json:"error,omitempty"`
	Output     string    `json:"output,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Event 通过 WS 推送给扩展。
type Event struct {
	Type string `json:"type"`
	Task *Task  `json:"task,omitempty"`
	Msg  string `json:"msg,omitempty"`
}

const (
	EventTaskUpdated = "task.updated"
	EventTaskDeleted = "task.deleted"
	EventPong        = "pong"
)
