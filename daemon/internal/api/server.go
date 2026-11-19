package api

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"bili-dl/internal/config"
	"bili-dl/internal/model"
	"bili-dl/internal/pipeline"
	"bili-dl/internal/queue"
	"bili-dl/internal/store"
	"bili-dl/internal/ws"

	log "github.com/sirupsen/logrus"
)

// Diag 是启动自检得到的依赖摘要，供 /api/diag 展示。
type Diag struct {
	FFmpeg     string
	OutputRoot string
}

// Server 提供 HTTP API 与 WS 推送。
type Server struct {
	cfg      config.Config
	store    *store.Store
	hub      *ws.Hub
	queue    *queue.Queue
	pipeline *pipeline.Pipeline
	diag     Diag
	mux      *http.ServeMux
}

// New 创建服务，并把 store 的变更事件桥接到 WS 广播。
func New(cfg config.Config, st *store.Store, q *queue.Queue, pl *pipeline.Pipeline, diag Diag) *Server {
	hub := ws.NewHub()
	s := &Server{cfg: cfg, store: st, hub: hub, queue: q, pipeline: pl, diag: diag, mux: http.NewServeMux()}

	// store 的每次 Upsert/Delete 都广播给所有 WS 客户端。
	st.SetEventHandler(func(ev model.Event) {
		data, err := json.Marshal(ev)
		if err != nil {
			return
		}
		hub.Broadcast(0x1, data)
	})

	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.withMiddleware(s.mux)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/api/health", s.handleHealth)
	s.mux.HandleFunc("/api/diag", s.handleDiag)
	s.mux.HandleFunc("/api/tasks", s.handleTasks)
	s.mux.HandleFunc("/api/task/", s.handleTaskByID)
	s.mux.HandleFunc("/api/settings", s.handleSettings)
	s.mux.HandleFunc("/ws", s.handleWS)
}

func (s *Server) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 本地扩展来源放行 CORS。
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if !s.authed(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) authed(r *http.Request) bool {
	// WS 无法自定义 Header，token 走 query。
	token := r.Header.Get("Authorization")
	token = strings.TrimPrefix(token, "Bearer ")
	if token == "" {
		token = r.URL.Query().Get("token")
	}
	return token == s.cfg.Token
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleDiag 返回 daemon 依赖与配置摘要，供扩展日志/探活展示。
func (s *Server) handleDiag(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "ok",
		"ffmpeg":        s.diag.FFmpeg,
		"output_root":   s.diag.OutputRoot,
		"merge_workers":  s.cfg.MergeWorkers,
		"download_tries": s.cfg.DownloadTries,
		"speed_limit_kbps": s.pipeline.SpeedLimit(),
	})
}

// handleSettings: GET 读取当前运行配置；POST 修改下载限速（KB/s，0=不限）。
func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			"speed_limit_kbps": s.pipeline.SpeedLimit(),
		})
	case http.MethodPost:
		var body struct {
			SpeedLimitKBPS *int `json:"speed_limit_kbps"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
			http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if body.SpeedLimitKBPS == nil {
			http.Error(w, "speed_limit_kbps 必填", http.StatusBadRequest)
			return
		}
		if *body.SpeedLimitKBPS < 0 {
			http.Error(w, "speed_limit_kbps 不能为负数", http.StatusBadRequest)
			return
		}
		s.pipeline.SetSpeedLimit(*body.SpeedLimitKBPS)
		writeJSON(w, http.StatusOK, map[string]any{
			"speed_limit_kbps": s.pipeline.SpeedLimit(),
		})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleTasks: GET 列出全部；POST 提交新任务。
func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.store.List())

	case http.MethodPost:
		var t model.Task
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
		if err := dec.Decode(&t); err != nil {
			http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if t.ID == "" || t.Video.URL == "" || t.Audio.URL == "" {
			http.Error(w, "id/video.url/audio.url 必填", http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(t.OutputPath) != "" {
			clean, err := pipeline.NormalizeOutputPath(t.OutputPath)
			if err != nil {
				http.Error(w, "output_path 非法: "+err.Error(), http.StatusBadRequest)
				return
			}
			t.OutputPath = clean
		}
		if _, exists := s.store.Get(t.ID); exists {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "任务已存在", "id": t.ID})
			return
		}
		t.State = model.StatePending
		t.Progress = 0
		t.Stage = "排队中"
		t.CreatedAt = time.Now()
		s.store.Upsert(&t)
		log.WithFields(log.Fields{"task": t.ID, "title": t.Title, "qn": t.QN}).
			Info("收到任务提交（HTTP）")
		s.queue.Enqueue(&t)
		writeJSON(w, http.StatusAccepted, t)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleTaskByID: /api/task/{id} 查询 / 删除 / 暂停 / 继续。
// 动作也可走 /api/task/{id}/pause 与 /api/task/{id}/resume（POST）。
func (s *Server) handleTaskByID(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/task/")
	rest = strings.TrimSpace(rest)
	action := ""
	if i := strings.LastIndex(rest, "/"); i >= 0 {
		action = rest[i+1:]
		rest = rest[:i]
	}
	id := strings.TrimSpace(rest)
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	switch {
	case action == "pause" && r.Method == http.MethodPost:
		s.pauseTask(w, id)
		return
	case action == "resume" && r.Method == http.MethodPost:
		s.resumeTask(w, id)
		return
	case action != "":
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		t, ok := s.store.Get(id)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, t)
	case http.MethodDelete:
		// 先中断正在执行的任务，再删状态；Pipeline 因 ctx 取消会保留分片，
		// 但移除语义要求彻底删除，这里主动清掉。
		canceled := s.queue.Cancel(id)
		s.removeParts(id)
		s.store.Delete(id)
		log.WithFields(log.Fields{"task": id, "was_running": canceled}).Info("任务已移除")
		writeJSON(w, http.StatusOK, map[string]string{"deleted": id})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) pauseTask(w http.ResponseWriter, id string) {
	t, ok := s.store.Get(id)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if t.State == model.StateDone || t.State == model.StateFailed {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "已结束的任务不能暂停", "state": string(t.State)})
		return
	}
	canceled := s.queue.Cancel(id)
	cp := *t
	cp.State = model.StatePaused
	cp.Stage = "已暂停"
	cp.StageText = ""
	cp.VideoSpeed = ""
	cp.AudioSpeed = ""
	s.store.Upsert(&cp)
	log.WithFields(log.Fields{"task": id, "was_running": canceled}).Info("任务已暂停，保留分片供继续")
	writeJSON(w, http.StatusOK, &cp)
}

func (s *Server) resumeTask(w http.ResponseWriter, id string) {
	t, ok := s.store.Get(id)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if t.State != model.StatePaused && t.State != model.StateFailed {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "仅暂停/失败的任务可继续", "state": string(t.State)})
		return
	}
	nt := *t
	nt.State = model.StatePending
	nt.Stage = "排队中"
	nt.Error = ""
	nt.Progress = 0
	s.store.Upsert(&nt)
	s.queue.Enqueue(&nt)
	log.WithField("task", id).Info("任务已继续，将从已下载分片续传")
	writeJSON(w, http.StatusOK, &nt)
}

// removeParts 删除任务的临时分片（视频/音频/合并产物 tmp）。
func (s *Server) removeParts(id string) {
	tmp := s.cfg.OutputRoot + "/.tmp/"
	os.Remove(tmp + id + "-v.part")
	os.Remove(tmp + id + "-a.part")
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	c, err := ws.Upgrade(w, r)
	if err != nil {
		log.WithError(err).Warn("WS 升级失败")
		return
	}
	defer c.Close()
	s.hub.Add(c)
	defer s.hub.Remove(c)
	log.WithFields(log.Fields{"remote": r.RemoteAddr, "clients": s.hub.Len()}).Info("WS 客户端已连接")

	// 连上即推全量快照，便于 popup 直接渲染队列。
	snap, _ := json.Marshal(model.Event{Type: "snapshot", Task: nil})
	full, _ := json.Marshal(struct {
		Type  string          `json:"type"`
		Tasks []*model.Task   `json:"tasks"`
	}{Type: "snapshot", Tasks: s.store.List()})
	_ = snap
	_ = c.WriteFrame(0x1, full)

	for {
		raw, err := c.ReadMessage(64 << 10)
		if err != nil {
			if err != ws.ErrClosed {
				log.WithError(err).Debug("WS 读取结束")
			}
			log.WithField("clients", s.hub.Len()).Info("WS 客户端已断开")
			return
		}
		// 应用层心跳：收到 PING 回 PONG，供 popup 判定连接是否存活。
		var msg struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(raw, &msg) == nil && msg.Type == "PING" {
			pong, _ := json.Marshal(map[string]string{"type": "PONG"})
			if err := c.WriteFrame(0x1, pong); err != nil {
				return
			}
		}
		// 其余消息（LIST / cancel）暂忽略。
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
