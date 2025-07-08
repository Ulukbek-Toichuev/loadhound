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
	"time"
)

type Executor struct {
	db              *SQLWrapper
	cfg             *RunTestConfig
	eventController *GeneralEventController
	pQuery          *PreparedQuery
	paramFuncs      []func() interface{}
}

func NewExecutor(globalCtx context.Context, cfg *RunTestConfig, pQuery *PreparedQuery, eventController *GeneralEventController) (*Executor, error) {
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
		return &Executor{db: db, cfg: cfg, eventController: eventController, pQuery: pQuery, paramFuncs: paramFuncs}, nil
	}
	return &Executor{db: db, cfg: cfg, eventController: eventController, pQuery: pQuery}, nil
}

func (s *Executor) Run(globalCtx context.Context) {
	var (
		workflowCfg = s.cfg.WorkflowConfig
		wg          = sync.WaitGroup{}
	)

	task := getTask(s.db, s.pQuery, s.paramFuncs)
	if task == nil {
		s.eventController.WriteErrMsg("task is nil", fmt.Errorf("failed to get tasks"))
		return
	}

	threadSlice := make([]*Thread, workflowCfg.Threads)
	for threadId := 1; threadId <= workflowCfg.Threads; threadId++ {
		if th := NewThread(threadId, workflowCfg, s.eventController, task); th != nil {
			threadSlice[threadId] = NewThread(threadId, workflowCfg, s.eventController, task)
			wg.Add(1)
		} else {
			s.eventController.WriteErrMsg("failed to create thread", fmt.Errorf("thread constructor return nil pointer"))
		}
	}

	for _, th := range threadSlice {
		if workflowCfg.Duration > 0 {
			deadlineCtx, cancel := context.WithDeadline(globalCtx, time.Now().Add(workflowCfg.Duration))
			defer cancel()
			go th.ExecByDur(deadlineCtx, &wg)
		} else if workflowCfg.Iterations > 0 {
			go th.ExecByIter(globalCtx, &wg, workflowCfg.Iterations)
		}
	}

	wg.Wait()
	if err := s.db.Close(); err != nil {
		s.eventController.WriteErrMsg("database close error", err)
	}
}

func getArgs(paramFuncs []func() interface{}) []interface{} {
	result := make([]interface{}, 0)
	for _, fn := range paramFuncs {
		result = append(result, fn())
	}
	return result
}

func getTask(db *SQLWrapper, pQuery *PreparedQuery, paramGenerators []func() interface{}) func(ctx context.Context) *QueryMetric {
	if db.stmt != nil {
		if pQuery.Type == TypeExec {
			return func(ctx context.Context) *QueryMetric {
				args := getArgs(paramGenerators)
				return db.StmtExec(ctx, args...)
			}
		} else if pQuery.Type == TypeQuery {
			return func(ctx context.Context) *QueryMetric {
				args := getArgs(paramGenerators)
				return db.StmtQuery(ctx, args...)
			}
		}
	} else {
		if pQuery.Type == TypeExec {
			return func(ctx context.Context) *QueryMetric {
				query, err := BuildQuery(pQuery.Tmpl)
				if err != nil {
					return nil
				}
				return db.Exec(ctx, query)
			}
		} else if pQuery.Type == TypeQuery {
			return func(ctx context.Context) *QueryMetric {
				query, err := BuildQuery(pQuery.Tmpl)
				if err != nil {
					return nil
				}
				return db.Query(ctx, query)
			}
		}
	}
	return nil
}
