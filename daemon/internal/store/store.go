package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"bili-dl/internal/model"

	log "github.com/sirupsen/logrus"
)

// op 是一次状态变更，全部串行进入同一个 writer，杜绝并发写。
type op struct {
	task  *model.Task
	kind  opKind
}

type opKind int

const (
	opUpsert opKind = iota
	opDelete
)

// Store 持有任务内存镜像，单一 writer goroutine 负责落盘。
type Store struct {
	mu      sync.RWMutex
	tasks   map[string]*model.Task
	ch      chan op
	path    string
	stop    chan struct{}
	done    chan struct{}
	onEvent func(model.Event)
}

// New 从 JSON 文件恢复任务，并启动 writer。
func New(path string, onEvent func(model.Event)) (*Store, error) {
	s := &Store{
		tasks:   make(map[string]*model.Task),
		ch:      make(chan op, 256),
		path:    path,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		onEvent: onEvent,
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	go s.loop()
	return s, nil
}

func (s *Store) load() error {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var list []*model.Task
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	for _, t := range list {
		// 进程重启后，未完成的任务无法续传（直链已过期），标记为失败。
		if t.State == model.StateDownloading || t.State == model.StateMerging || t.State == model.StatePending || t.State == model.StatePaused {
			t.State = model.StateFailed
			t.Error = "daemon 重启，任务中断；请重新下发"
		}
		s.tasks[t.ID] = t
	}
	return nil
}

// SetEventHandler 设置状态变更回调（用于桥接 WS 广播）。
// 必须在 New 之后、事件产生之前调用。
func (s *Store) SetEventHandler(fn func(model.Event)) {
	s.mu.Lock()
	s.onEvent = fn
	s.mu.Unlock()
}

// Upsert 写入或更新任务。
func (s *Store) Upsert(t *model.Task) {
	t.UpdatedAt = time.Now()
	cp := *t
	s.ch <- op{task: &cp, kind: opUpsert}
}

// Delete 删除任务。
func (s *Store) Delete(id string) {
	s.ch <- op{task: &model.Task{ID: id}, kind: opDelete}
}

// Get 返回任务副本，避免调用方改到内部状态。
func (s *Store) Get(id string) (*model.Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, false
	}
	cp := *t
	return &cp, true
}

// List 返回全部任务副本，按创建时间升序（相同时间按 ID）稳定排序。
// 关键：map 遍历顺序随机，若不排序，popup 每次刷新都会上下跳动。
func (s *Store) List() []*model.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		cp := *t
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

func (s *Store) loop() {
	defer close(s.done)
	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()
	dirty := false

	flush := func() {
		if !dirty {
			return
		}
		if err := s.persist(); err != nil {
			log.WithError(err).Error("持久化任务失败")
			return
		}
		dirty = false
	}

	for {
		select {
		case o := <-s.ch:
			s.mu.Lock()
			switch o.kind {
			case opUpsert:
				s.tasks[o.task.ID] = o.task
			case opDelete:
				delete(s.tasks, o.task.ID)
			}
			s.mu.Unlock()
			dirty = true
			s.mu.RLock()
			fn := s.onEvent
			s.mu.RUnlock()
			if fn != nil {
				evType := model.EventTaskUpdated
				if o.kind == opDelete {
					evType = model.EventTaskDeleted
				}
				fn(model.Event{Type: evType, Task: o.task})
			}
		case <-ticker.C:
			flush()
		case <-s.stop:
			flush()
			return
		}
	}
}

// persist 由单一 writer 调用：写临时文件后原子 rename。
func (s *Store) persist() error {
	s.mu.RLock()
	list := make([]*model.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		cp := *t
		list = append(list, &cp)
	}
	s.mu.RUnlock()

	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Close 停止 writer 并完成最后一次落盘。
func (s *Store) Close() {
	close(s.stop)
	<-s.done
}
