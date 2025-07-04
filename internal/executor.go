/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"text/template"
	"time"
)

type Executor interface {
	Run(ctx context.Context) *MetricEngine
}

type SimpleExecutor struct {
	db       *SQLWrapper
	cfg      *RunTestConfig
	genEvent *GeneralEventController
	pQuery   *PreparedQuery
}

func NewSimpleExecutor(globalCtx context.Context, cfg *RunTestConfig, pQuery *PreparedQuery, g *GeneralEventController) (*SimpleExecutor, error) {
	db, err := NewSQLWrapper(globalCtx, cfg.DbConfig, pQuery.Tmpl)
	if err != nil {
		return nil, err
	}
	if db.stmt != nil {
		return nil, errors.New("prepared statement currently unavailable")
	}
	return &SimpleExecutor{db: db, cfg: cfg, genEvent: g, pQuery: pQuery}, nil
}

func (s *SimpleExecutor) Run(globalCtx context.Context) *MetricEngine {
	var (
		metricEngine = NewMetricEngine(1000)
		workflowCfg  = s.cfg.WorkflowConfig
		wg           = &sync.WaitGroup{}
		barrier      = NewBarrier(workflowCfg.Threads)
	)

	wg.Add(workflowCfg.Threads)
	threadSlice := make([]*Thread, workflowCfg.Threads)
	for idx := range workflowCfg.Threads {
		threadSlice[idx] = NewThread(idx, wg, metricEngine, barrier, workflowCfg.Pacing, s.pQuery.Tmpl, s.genEvent, getQueryFunc(s.db, s.pQuery))
	}

	var deadlineCtxCancel func() error
	if workflowCfg.Duration > 0 {
		deadlineCtx, cancel := context.WithDeadline(globalCtx, time.Now().Add(workflowCfg.Duration))
		deadlineCtxCancel = func() error {
			cancel()
			return nil
		}
		for _, th := range threadSlice {
			go th.ExecByDur(deadlineCtx)
		}
	} else if workflowCfg.Iterations > 0 {
		itersPerThread := distributeIterations(workflowCfg.Iterations, workflowCfg.Threads)
		for idx, th := range threadSlice {
			go th.ExecByIter(globalCtx, itersPerThread[idx])
		}
	}

	wg.Wait()
	err := closeResources(s.db.Close, deadlineCtxCancel)
	if err != nil {
		s.genEvent.WriteErrMsgWithBar("failed to close resources", err)
	}
	return metricEngine
}

func closeResources(funcs ...func() error) error {
	for _, fn := range funcs {
		if fn == nil {
			continue
		}
		err := fn()
		if err != nil {
			return err
		}
	}
	return nil
}

type Thread struct {
	id           int
	wg           *sync.WaitGroup
	metricEngine *MetricEngine
	barrier      *Barrier
	pacing       time.Duration
	tmpl         *template.Template
	genEvent     *GeneralEventController
	queryFunc    func(globalCtx context.Context, query string) *QueryMetric
}

func NewThread(id int,
	wg *sync.WaitGroup,
	metricEngine *MetricEngine,
	barrier *Barrier,
	pacing time.Duration,
	tmpl *template.Template,
	genEvent *GeneralEventController,
	queryFunc func(globalCtx context.Context, query string) *QueryMetric) *Thread {
	// Increase active thread in metric engine
	metricEngine.AddThread()
	return &Thread{
		id:           id,
		wg:           wg,
		metricEngine: metricEngine,
		barrier:      barrier,
		pacing:       pacing,
		tmpl:         tmpl,
		genEvent:     genEvent,
		queryFunc:    queryFunc}
}

func (t *Thread) ExecByDur(ctx context.Context) {
	defer t.wg.Done()
	t.barrier.Touch()
	for {
		select {
		case <-ctx.Done():
			t.metricEngine.AddFailedThread()
			return
		default:
		}
		err := t.exec(ctx)
		if err != nil {
			t.metricEngine.AddFailedThread()
			t.genEvent.WriteErrMsgWithBar(fmt.Sprintf("thread-%d prematurely end work, get error", t.id), err)
			return
		}
		t.metricEngine.AddIter()
	}
}

func (t *Thread) ExecByIter(ctx context.Context, iter int) {
	defer t.wg.Done()
	t.barrier.Touch()
	for range iter {
		select {
		case <-ctx.Done():
			t.metricEngine.AddFailedThread()
			return
		default:
		}
		err := t.exec(ctx)
		if err != nil {
			t.metricEngine.AddFailedThread()
			t.genEvent.WriteErrMsgWithBar(fmt.Sprintf("thread-%d prematurely end work, get error", t.id), err)
			return
		}

		t.metricEngine.AddIter()
	}
}

func (t *Thread) exec(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	query, err := BuildQuery(t.tmpl)
	if err != nil {
		return err
	}
	start := time.Now()
	metric := t.queryFunc(ctx, query)

	// Submit metrics about query
	t.metricEngine.AddQueryMetric(metric)

	// Write to log query metric and increment progress bar
	t.genEvent.WriteQueryStat(metric)
	t.genEvent.Increment()

	// Set pacing
	pacing(start, t.pacing)
	return nil
}

func getQueryFunc(db *SQLWrapper, pQuery *PreparedQuery) func(globalCtx context.Context, query string) *QueryMetric {
	if pQuery.QueryType == QueryTypeExec.String() {
		return db.ExecWithLatency
	} else if pQuery.QueryType == QueryTypeRead.String() {
		return db.QueryRowsWithLatency
	}
	return nil
}

func distributeIterations(iterations, threads int) []int {
	result := make([]int, threads)
	for i := 0; i < iterations; i++ {
		result[i%threads]++
	}
	return result
}

func pacing(start time.Time, pacing time.Duration) {
	if pacing == 0 {
		return
	}
	elapsed := time.Since(start)
	remaining := pacing - elapsed
	if remaining > 0 {
		time.Sleep(remaining)
	}
}

type Barrier struct {
	n    int
	cond *sync.Cond
}

func NewBarrier(n int) *Barrier {
	b := &Barrier{n: n}
	b.cond = sync.NewCond(&sync.Mutex{})
	return b
}

func (b *Barrier) Touch() {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()
	if b.n == 0 {
		return
	}
	b.n--
	for b.n > 0 {
		b.cond.Wait()
	}
	b.cond.Broadcast()
}
