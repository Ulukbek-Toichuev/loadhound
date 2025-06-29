/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package executor

import (
	"context"
	"fmt"
	"sync"
	"text/template"
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal"
	"github.com/Ulukbek-Toichuev/loadhound/internal/db"
	"github.com/Ulukbek-Toichuev/loadhound/internal/parse"
	"github.com/Ulukbek-Toichuev/loadhound/internal/perform"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
)

const queryChLen int = 10

type SimpleTestExecutor struct {
	cfg             *internal.RunTestConfig
	queryCh         chan *stat.QueryStat
	p               perform.Performer
	eventController *internal.GeneralEventController
}

func NewSimpleTestExecutor(cfg *internal.RunTestConfig, performer perform.Performer, eventController *internal.GeneralEventController) *SimpleTestExecutor {
	return &SimpleTestExecutor{
		cfg:             cfg,
		queryCh:         make(chan *stat.QueryStat, queryChLen*cfg.TestConfig.Workers),
		p:               performer,
		eventController: eventController,
	}
}

func BuildSimpleExecutor(ctx context.Context, runCfg *internal.RunTestConfig, tmpl *template.Template, queryType string, eventController *internal.GeneralEventController) (*SimpleTestExecutor, error) {
	db, err := getDBInstance(ctx, &runCfg.DbConfig, tmpl)
	if err != nil {
		return nil, err
	}

	performer, err := getPerformer(queryType, db, tmpl, runCfg.DbConfig.UseStmt)
	if err != nil {
		return nil, err
	}
	return NewSimpleTestExecutor(runCfg, performer, eventController), nil
}

func (e *SimpleTestExecutor) Run(ctx context.Context) *stat.Stat {
	stats := stat.NewStat()
	wgStats := startStatsCollector(ctx, stats, e.queryCh, e.eventController)

	wgWorkers := e.start(ctx)
	if wgWorkers == nil {
		close(e.queryCh)
		return stats
	}

	wgWorkers.Wait()
	close(e.queryCh)
	wgStats.Wait()

	e.p.Close()
	return stats
}

func (e *SimpleTestExecutor) startWorkersOnDur(ctx context.Context) *sync.WaitGroup {
	var wg sync.WaitGroup
	startSignal := make(chan struct{})

	startWorker := func(wg *sync.WaitGroup, workerID int) {
		defer wg.Done()
		<-startSignal
		timeout := time.After(e.cfg.TestConfig.Duration)
		for {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-timeout:
				return
			default:
				execQuery(ctx, e, workerID)
				e.eventController.Increment()
			}
		}
	}

	for i := 0; i < e.cfg.TestConfig.Workers; i++ {
		wg.Add(1)
		workerID := i
		go startWorker(&wg, workerID)
	}

	close(startSignal)
	return &wg
}

func (e *SimpleTestExecutor) startWorkersOnIters(ctx context.Context) *sync.WaitGroup {
	var wg sync.WaitGroup
	startSignal := make(chan struct{})
	itersPerWorker := distributeIterations(e.cfg.TestConfig.Iterations, e.cfg.TestConfig.Workers)

	startWorker := func(wg *sync.WaitGroup, workerID, iterCount int) {
		defer wg.Done()
		if iterCount == 0 {
			return
		}
		<-startSignal
		for i := 0; i < iterCount; i++ {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			default:
				execQuery(ctx, e, workerID)
				e.eventController.Increment()
			}
		}
	}

	for i := 0; i < e.cfg.TestConfig.Workers; i++ {
		wg.Add(1)
		workerID := i
		go startWorker(&wg, workerID, itersPerWorker[workerID])
	}

	close(startSignal)
	return &wg
}

func execQuery(ctx context.Context, e *SimpleTestExecutor, workerID int) {
	select {
	case <-ctx.Done():
		return
	default:
		start := time.Now()
		queryStat, err := e.p.Perform(ctx)
		if err != nil {
			// TODO fix
		}
		if queryStat != nil {
			queryStat.WorkerID = workerID
			e.queryCh <- queryStat
		}
		pacing(start, e.cfg.TestConfig.Pacing)
	}
}

func (e *SimpleTestExecutor) start(ctx context.Context) *sync.WaitGroup {
	switch {
	case e.cfg.TestConfig.Iterations > 0:
		return e.startWorkersOnIters(ctx)
	case e.cfg.TestConfig.Duration > 0:
		return e.startWorkersOnDur(ctx)
	default:
		return nil
	}
}

func getPerformer(queryType string, conn db.SQLExecutor, tmpl *template.Template, isStmt bool) (perform.Performer, error) {
	switch queryType {
	case parse.QueryTypeRead.String():
		return perform.NewSQLPerformQueryRows(conn, tmpl, isStmt), nil
	case parse.QueryTypeExec.String():
		return perform.NewSQLPerformExec(conn, tmpl, isStmt), nil
	default:
		return nil, perform.NewPerformerError(fmt.Sprintf("unsupported query type: %s", queryType), nil)
	}
}

func distributeIterations(total, workers int) []int {
	result := make([]int, workers)
	for i := 0; i < total; i++ {
		result[i%workers]++
	}
	return result
}

func pacing(start time.Time, pacing time.Duration) {
	elapsed := time.Since(start)
	remaining := pacing - elapsed
	if remaining > 0 {
		time.Sleep(remaining)
	}
}

func startStatsCollector(ctx context.Context, stats *stat.Stat, queryCh chan *stat.QueryStat, eventController *internal.GeneralEventController) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case q, ok := <-queryCh:
				if !ok {
					return
				}
				eventController.WriteQueryStat(q)
				stats.SubmitStat(q)
			}
		}
	}(&wg)

	return &wg
}

func getDBInstance(ctx context.Context, dbConfig *internal.DbConfig, tmpl *template.Template) (*db.SQLWrapper, error) {
	if dbConfig.Driver == "postgres" || dbConfig.Driver == "mysql" {
		return db.NewSQLWrapper(ctx, dbConfig, tmpl)
	}
	return nil, fmt.Errorf("failed to get db instance, unknown driver type")
}
