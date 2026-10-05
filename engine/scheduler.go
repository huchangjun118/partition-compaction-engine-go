package engine

import (
	"sort"
	"sync"
	"time"
)

// scheduler 后台调度：周期性地触发保留期淘汰、分层迁移与小文件合并。
// 每个分区的合并独立推进（各自 goroutine + 每分区互斥），慢分区不阻塞其他分区。
type scheduler struct {
	eng    *Engine
	stopCh chan struct{}
	done   chan struct{}
	kickCh chan struct{}

	passMu  sync.Mutex
	mu      sync.Mutex
	stats   map[string]*PartitionStatus
	running map[string]bool // 正在合并的分区
	sem     chan struct{}   // 合并并发度
	jobs    sync.WaitGroup  // 仅跟踪合并任务，不含调度循环本身
}

func newScheduler(eng *Engine) *scheduler {
	return &scheduler{
		eng:     eng,
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
		kickCh:  make(chan struct{}, 1),
		stats:   map[string]*PartitionStatus{},
		running: map[string]bool{},
		sem:     make(chan struct{}, eng.config().CompactWorkers),
	}
}

func (s *scheduler) start() {
	go s.loop()
}

func (s *scheduler) stop() {
	close(s.stopCh)
	<-s.done
	s.jobs.Wait() // 等待进行中的合并结束，避免关闭后还有写盘
}

func (s *scheduler) loop() {
	defer close(s.done)
	cfg := s.eng.config()
	ticker := time.NewTicker(cfg.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.pass()
		case <-s.kickCh:
			s.pass()
		}
	}
}

// pass 执行一轮调度：淘汰、迁移、合并。同一时刻只允许一轮。
func (s *scheduler) pass() {
	s.passMu.Lock()
	defer s.passMu.Unlock()
	select {
	case <-s.stopCh:
		return
	default:
	}
	if err := s.eng.sweepRetention(); err != nil {
		s.markError("*retention*", err)
	}
	if err := s.eng.sweepTiering(); err != nil {
		s.markError("*tiering*", err)
	}
	s.scheduleCompactions()
}

// scheduleCompactions 为达到阈值的分区各启动一个合并任务。
func (s *scheduler) scheduleCompactions() {
	cfg := s.eng.config()
	counts := s.eng.partitionFileCounts()
	for p, n := range counts {
		if n < cfg.CompactThreshold {
			continue
		}
		s.mu.Lock()
		if s.running[p] {
			s.mu.Unlock()
			continue
		}
		s.running[p] = true
		st := s.statLocked(p)
		st.State = JobPending
		st.FileCount = n
		s.mu.Unlock()

		s.jobs.Add(1)
		go func(partition string) {
			defer s.jobs.Done()
			s.sem <- struct{}{}
			defer func() { <-s.sem }()
			s.mu.Lock()
			st := s.statLocked(partition)
			st.State = JobRunning
			st.LastRun = time.Now()
			s.mu.Unlock()

			err := s.eng.compactPartition(partition)

			s.mu.Lock()
			defer s.mu.Unlock()
			delete(s.running, partition)
			st = s.statLocked(partition)
			if err != nil {
				st.State = JobFailed
				st.LastError = err.Error()
			} else {
				st.State = JobDone
				st.LastError = ""
			}
			st.FileCount = s.eng.partitionFileCounts()[partition]
		}(p)
	}
}

func (s *scheduler) statLocked(p string) *PartitionStatus {
	st, ok := s.stats[p]
	if !ok {
		st = &PartitionStatus{Partition: p, State: JobIdle}
		s.stats[p] = st
	}
	return st
}

func (s *scheduler) markError(tag string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.statLocked(tag)
	st.State = JobFailed
	st.LastError = err.Error()
	st.LastRun = time.Now()
}

func (s *scheduler) status() []PartitionStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]PartitionStatus, 0, len(s.stats))
	for _, st := range s.stats {
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })
	return out
}

// kick 异步触发一轮调度。
func (s *scheduler) kick() {
	select {
	case s.kickCh <- struct{}{}:
	default:
	}
}

// sweepNow 同步执行一轮调度（测试用），并等待所有已触发的合并完成。
func (s *scheduler) sweepNow() {
	s.pass()
	s.jobs.Wait()
}
