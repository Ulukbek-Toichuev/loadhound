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
	paramFuncs []func() interface{}
}

func NewSimpleExecutor(globalCtx context.Context, cfg *RunTestConfig, pQuery *PreparedQuery, g *GeneralEventController) (*SimpleExecutor, error) {
	db, err := NewSQLWrapper(globalCtx, cfg.DbConfig, pQuery.Tmpl)
	if err != nil {
		return nil, err
	}
	if db.stmt != nil {
		funcs := GetFuncsName(pQuery.RawSQL)
		paramFuncs, err := CollectFuncs(funcs)
		if err != nil {
			return nil, err
		}
		return &SimpleExecutor{db: db, cfg: cfg, genEvent: g, pQuery: pQuery, paramFuncs: paramFuncs}, nil
	}
	return &SimpleExecutor{db: db, cfg: cfg, genEvent: g, pQuery: pQuery}, nil
}

func (s *SimpleExecutor) Run(globalCtx context.Context) *MetricEngine {
	var (
		metricEngine = NewMetricEngine(1000)
		workflowCfg  = s.cfg.WorkflowConfig
		wg           = sync.WaitGroup{}
	)
	wg.Add(workflowCfg.Threads)
	threadSlice := make([]*Thread, workflowCfg.Threads)
	for idx := range workflowCfg.Threads {
		fn := getTask(s.db, s.pQuery, s.paramFuncs)
		if fn == nil {
			s.genEvent.WriteErrMsg("task is nil", fmt.Errorf("failed to create new thread"))
			continue
		}
		threadSlice[idx] = NewThread(idx, &wg, metricEngine, workflowCfg.Pacing, s.pQuery.Tmpl, s.genEvent, fn)
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
	pacing       time.Duration
	tmpl         *template.Template
	genEvent     *GeneralEventController
	task         func(globalCtx context.Context) *QueryMetric
	once         sync.Once
}

func NewThread(id int,
	wg *sync.WaitGroup,
	metricEngine *MetricEngine,
	pacing time.Duration,
	tmpl *template.Template,
	genEvent *GeneralEventController,
	task func(globalCtx context.Context) *QueryMetric) *Thread {
	// Increase active thread in metric engine
	metricEngine.AddThread()
	return &Thread{
		id:           id,
		wg:           wg,
		metricEngine: metricEngine,
		pacing:       pacing,
		tmpl:         tmpl,
		genEvent:     genEvent,
		task:         task,
	}
}

func (t *Thread) ExecByDur(ctx context.Context) {
	defer t.wg.Done()
	t.once.Do(t.metricEngine.Activate)
	for {
		select {
		case <-ctx.Done():
			t.genEvent.WriteErrMsg(fmt.Sprintf("thread %d end work", t.id), ctx.Err())
			return
		default:
		}
		start := time.Now()
		metric, err := t.exec(ctx)
		if err != nil {
			t.metricEngine.AddFailedThread()
			t.genEvent.WriteErrMsgWithBar(fmt.Sprintf("thread-%d prematurely end work, get error", t.id), err)
			return
		}

		// Write to log query metric
		metric.ThreadID = t.id
		t.genEvent.WriteQueryStat(metric)

		// Submit metrics about query
		t.metricEngine.AddQueryMetric(metric)

		// Set pacing
		pacing(start, t.pacing)
	}
}

func (t *Thread) ExecByIter(ctx context.Context, iter int) {
	defer t.wg.Done()
	t.once.Do(t.metricEngine.Activate)
	for range iter {
		select {
		case <-ctx.Done():
			t.genEvent.WriteErrMsg(fmt.Sprintf("thread %d end work", t.id), ctx.Err())
			return
		default:
		}
		start := time.Now()
		metric, err := t.exec(ctx)
		if err != nil {
			t.metricEngine.AddFailedThread()
			t.genEvent.WriteErrMsgWithBar(fmt.Sprintf("thread-%d prematurely end work, get error", t.id), err)
			return
		}
		// Write to log query metric
		metric.ThreadID = t.id
		t.genEvent.WriteQueryStat(metric)

		// Submit metrics about query
		t.metricEngine.AddQueryMetric(metric)

		// Set pacing
		pacing(start, t.pacing)
	}
}

func (t *Thread) exec(ctx context.Context) (*QueryMetric, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return t.task(ctx), nil
}

func getArgs(paramFuncs []func() interface{}) []interface{} {
	result := make([]interface{}, 0)
	for _, fn := range paramFuncs {
		result = append(result, fn())
	}
	return result
}

func getTask(db *SQLWrapper, pQuery *PreparedQuery, pStmtFuncs []func() interface{}) func(globalCtx context.Context) *QueryMetric {
	if db.stmt != nil {
		if pQuery.Type == TypeExec {
			return func(globalCtx context.Context) *QueryMetric {
				args := getArgs(pStmtFuncs)
				return db.StmtExec(globalCtx, args...)
			}
		} else if pQuery.Type == TypeQuery {
			return func(globalCtx context.Context) *QueryMetric {
				args := getArgs(pStmtFuncs)
				return db.StmtQuery(globalCtx, args...)
			}
		}
	} else {
		if pQuery.Type == TypeExec {
			return func(globalCtx context.Context) *QueryMetric {
				query, err := BuildQuery(pQuery.Tmpl)
				if err != nil {
					return nil
				}
				return db.Exec(globalCtx, query)
			}
		} else if pQuery.Type == TypeQuery {
			return func(globalCtx context.Context) *QueryMetric {
				query, err := BuildQuery(pQuery.Tmpl)
				if err != nil {
					return nil
				}
				return db.Query(globalCtx, query)
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
	if elapsed >= pacing {
		return
	}
	remaining := pacing - elapsed
	if remaining > 0 {
		time.Sleep(remaining)
	}
}
