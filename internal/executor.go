/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"context"
	"fmt"
	"sync"
	"text/template"
	"time"
)

type Executor interface {
	Run(ctx context.Context) *MetricEngine
}

type SimpleExecutor struct {
	db         *SQLWrapper
	cfg        *RunTestConfig
	genEvent   *GeneralEventController
	pQuery     *PreparedQuery
	pStmtFuncs []func() interface{}
}

func NewSimpleExecutor(globalCtx context.Context, cfg *RunTestConfig, pQuery *PreparedQuery, g *GeneralEventController) (*SimpleExecutor, error) {
	db, err := NewSQLWrapper(globalCtx, cfg.DbConfig, pQuery.Tmpl)
	if err != nil {
		return nil, err
	}
	if db.stmt != nil {
		funcs := GetFuncs(pQuery.RawSQL)
		pStmtFuncs, err := CollectFuncs(funcs)
		if err != nil {
			return nil, err
		}
		return &SimpleExecutor{db: db, cfg: cfg, genEvent: g, pQuery: pQuery, pStmtFuncs: pStmtFuncs}, nil
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
		fn := getQueryFunc(s.db, s.pQuery, s.pStmtFuncs)
		if fn == nil {
			s.genEvent.WriteErrMsg("query func is nil", fmt.Errorf("failed to create new thread"))
			continue
		}
		threadSlice[idx] = NewThread(idx, wg, metricEngine, barrier, workflowCfg.Pacing, s.pQuery.Tmpl, s.genEvent, fn)
	}

	if workflowCfg.Duration > 0 {
		deadlineCtx, cancel := context.WithDeadline(globalCtx, time.Now().Add(workflowCfg.Duration))
		defer cancel()
		for _, th := range threadSlice {
			go th.ExecByDur(deadlineCtx)
		}
	} else if workflowCfg.Iterations > 0 {
		for _, th := range threadSlice {
			go th.ExecByIter(globalCtx, workflowCfg.Iterations)
		}
	}

	wg.Wait()
	if err := s.db.Close(); err != nil {
		panic(err)
	}
	return metricEngine
}

type Thread struct {
	id           int
	wg           *sync.WaitGroup
	metricEngine *MetricEngine
	barrier      *Barrier
	pacing       time.Duration
	tmpl         *template.Template
	genEvent     *GeneralEventController
	queryFunc    func(globalCtx context.Context) *QueryMetric
	once         sync.Once
}

func NewThread(id int,
	wg *sync.WaitGroup,
	metricEngine *MetricEngine,
	barrier *Barrier,
	pacing time.Duration,
	tmpl *template.Template,
	genEvent *GeneralEventController,
	queryFunc func(globalCtx context.Context) *QueryMetric) *Thread {
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
		queryFunc:    queryFunc,
	}
}

func (t *Thread) ExecByDur(ctx context.Context) {
	defer t.wg.Done()
	t.barrier.Touch()
	t.once.Do(t.metricEngine.Activate)
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
	t.once.Do(t.metricEngine.Activate)
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
	start := time.Now()
	metric := t.queryFunc(ctx)

	// Write to log query metric and increment progress bar
	t.genEvent.WriteQueryStat(metric)
	t.genEvent.Increment()

	// Set pacing
	pacing(start, t.pacing)

	// Submit metrics about query
	t.metricEngine.AddQueryMetric(metric)
	return nil
}

func getArgs(pStmtFuncs []func() interface{}) []interface{} {
	result := make([]interface{}, 0)
	for _, fn := range pStmtFuncs {
		result = append(result, fn())
	}
	return result
}

func getQueryFunc(db *SQLWrapper, pQuery *PreparedQuery, pStmtFuncs []func() interface{}) func(globalCtx context.Context) *QueryMetric {
	if db.stmt != nil {
		if pQuery.QueryType == QueryTypeExec.String() {
			return func(globalCtx context.Context) *QueryMetric {
				args := getArgs(pStmtFuncs)
				return db.StmtExecWithLatency(globalCtx, args...)
			}
		} else if pQuery.QueryType == QueryTypeRead.String() {
			return func(globalCtx context.Context) *QueryMetric {
				args := getArgs(pStmtFuncs)
				return db.StmtQueryRowsWithLatency(globalCtx, args...)
			}
		}
	} else {
		if pQuery.QueryType == QueryTypeExec.String() {
			return func(globalCtx context.Context) *QueryMetric {
				query, err := BuildQuery(pQuery.Tmpl)
				if err != nil {
					return nil
				}
				return db.ExecWithLatency(globalCtx, query)
			}
		} else if pQuery.QueryType == QueryTypeRead.String() {
			return func(globalCtx context.Context) *QueryMetric {
				query, err := BuildQuery(pQuery.Tmpl)
				if err != nil {
					return nil
				}
				return db.QueryRowsWithLatency(globalCtx, query)
			}
		}

	}
	return nil
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
