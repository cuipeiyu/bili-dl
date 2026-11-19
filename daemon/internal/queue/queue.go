// Package queue 调度下载任务。
// 下载阶段全局串行（一次一个任务），合并阶段由独立 worker 池并行。
package queue

import (
	"context"
	"sync"

	"bili-dl/internal/model"

	log "github.com/sirupsen/logrus"
)

// Executor 由 pipeline 层实现，负责真实下载与合并。
// ctx 用于暂停/移除时中断下载与合并；done 在任务彻底结束（含合并）后调用，
// 用于释放运行状态。注意 ctx 不因 Run 返回而取消——合并是异步的。
type Executor interface {
	Run(ctx context.Context, t *model.Task, update func(*model.Task), done func()) error
}

// running 记录一个正在执行任务的取消函数。
type running struct {
	cancel context.CancelFunc
}

// Queue 两级调度：download 单 worker，merge 由 Executor 内部并行。
type Queue struct {
	exec    Executor
	store   interface{ Upsert(*model.Task) }
	jobs    chan *model.Task
	running map[string]running
	mu      sync.Mutex
}

func New(exec Executor, st interface{ Upsert(*model.Task) }) *Queue {
	q := &Queue{
		exec:    exec,
		store:   st,
		jobs:    make(chan *model.Task, 512),
		running: make(map[string]running),
	}
	go q.loop()
	return q
}

// Enqueue 提交任务。重复 ID（已在执行或已在队列中）会被忽略。
func (q *Queue) Enqueue(t *model.Task) {
	q.mu.Lock()
	if _, busy := q.running[t.ID]; busy {
		q.mu.Unlock()
		log.WithField("task", t.ID).Warn("任务已在队列中，忽略重复提交")
		return
	}
	q.mu.Unlock()

	depth := len(q.jobs)
	log.WithFields(log.Fields{"task": t.ID, "title": t.Title, "queued": depth + 1}).
		Info("任务已入队")
	q.jobs <- t
}

// IsRunning 任务是否正在下载/合并阶段执行。
func (q *Queue) IsRunning(id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.running[id]
	return ok
}

// Cancel 中断正在执行的任务（暂停或移除共用）。返回是否命中正在执行的任务。
// 排队中尚未开始的任务不在 running 表里，由调用方直接从 store 删除即可。
func (q *Queue) Cancel(id string) bool {
	q.mu.Lock()
	r, ok := q.running[id]
	q.mu.Unlock()
	if !ok {
		return false
	}
	r.cancel()
	log.WithField("task", id).Info("已向运行中的任务发送取消信号")
	return true
}

// finish 释放任务的运行状态并释放 context 资源。
// 幂等：重复调用安全。
func (q *Queue) finish(id string, cancel context.CancelFunc) {
	q.mu.Lock()
	delete(q.running, id)
	q.mu.Unlock()
	cancel()
}

func (q *Queue) loop() {
	for t := range q.jobs {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			q.mu.Lock()
			q.running[t.ID] = running{cancel: cancel}
			q.mu.Unlock()

			done := func() { q.finish(t.ID, cancel) }
			update := func(nt *model.Task) {
				q.store.Upsert(nt)
			}
			log.WithFields(log.Fields{"task": t.ID, "pending": len(q.jobs)}).
				Info("队列开始处理任务（全局串行下载）")
			err := q.exec.Run(ctx, t, update, done)
			// Run 返回只代表下载调度结束（成功投递合并 或 失败）。
			// 失败/取消时合并不会发生，需在这里释放；成功投递合并时由 doMerge 调 done 释放。
			if err != nil {
				if ctx.Err() != nil {
					log.WithField("task", t.ID).Info("任务被取消，跳过失败标记")
				} else {
					log.WithField("task", t.ID).WithError(err).Error("任务下载阶段失败")
				}
				done()
			}
		}()
	}
}
