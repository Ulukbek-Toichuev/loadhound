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

	"github.com/Ulukbek-Toichuev/loadhound/internal/db"
	"github.com/Ulukbek-Toichuev/loadhound/internal/model"
	"github.com/Ulukbek-Toichuev/loadhound/internal/parse"
	"github.com/Ulukbek-Toichuev/loadhound/internal/perform"
	progress "github.com/Ulukbek-Toichuev/loadhound/internal/progressbar"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"

	"github.com/rs/zerolog"
)

const queryChanLen int = 10

type QuickExecutor struct {
	cfg       *model.QuickRun
	queryChan chan *stat.QueryStat
	pgBar     progress.ProgressReporter
	p         perform.Performer
}

func NewQuickExecutor(cfg *model.QuickRun, performer perform.Performer, reporter progress.ProgressReporter) *QuickExecutor {
	return &QuickExecutor{
		cfg:       cfg,
		queryChan: make(chan *stat.QueryStat, queryChanLen*cfg.Workers),
		p:         performer,
		pgBar:     reporter,
	}
}

func getDBInstance(ctx context.Context, cfg *model.QuickRun, tmpl *template.Template) (*db.SQLWrapper, error) {
	if cfg.Driver == "postgres" || cfg.Driver == "mysql" {
		return db.NewSQLWrapper(ctx, cfg, tmpl)
	}
	return nil, fmt.Errorf("failed to get db instance, unknown driver type")
}

func BuildQuickExecutor(ctx context.Context, cfg *model.QuickRun) (*QuickExecutor, error) {
	preparedQuery, err := getPreparedQuery(cfg)
	if err != nil {
		return nil, err
	}
	cfg.QueryTemplate = preparedQuery.RawSQL

	cfg.Logger.Info().Msg("getting query template")
	tmpl, err := parse.GetQueryTemplate(preparedQuery.RawSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to get template: %v", err)
	}
	db, err := getDBInstance(ctx, cfg, tmpl)
	if err != nil {
		return nil, err
	}

	performer, err := getPerformer(preparedQuery.QueryType, db, tmpl, cfg.UseStmt)
	if err != nil {
		return nil, err
	}

	pgBar, err := getProgressBar(cfg)
	if err != nil {
		return nil, err
	}
	return NewQuickExecutor(cfg, performer, pgBar), nil
}

func (e *QuickExecutor) Run(ctx context.Context) *stat.Stat {
	if e.pgBar == nil {
		e.cfg.Logger.Error().Msg("failed to start progress bar")
		return nil
	}
	e.pgBar.Start(ctx)
	defer e.pgBar.Stop()

	stats := stat.NewStat()
	wgStats := startStatsCollector(ctx, stats, e.queryChan)

	wgWorkers := e.start(ctx)
	if wgWorkers == nil {
		e.log().Error().Msg("failed to start test")
		close(e.queryChan)
		return stats
	}

	wgWorkers.Wait()
	close(e.queryChan)
	wgStats.Wait()

	e.p.Close()
	return stats
}

func (e *QuickExecutor) startWorkersOnDur(ctx context.Context) *sync.WaitGroup {
	var wg sync.WaitGroup
	startSignal := make(chan struct{})

	startWorker := func(wg *sync.WaitGroup, workerID int) {
		defer wg.Done()
		<-startSignal
		timeout := time.After(e.cfg.Duration)
		for {
			if ctx.Err() != nil {
				e.log().Error().Int("worker-id", workerID).Err(ctx.Err()).Msg("context err from worker")
				return
			}
			select {
			case <-ctx.Done():
				e.log().Info().Int("worker-id", workerID).Msg("getting 'Done' signal from the context")
				return
			case <-timeout:
				return
			default:
				start := time.Now()
				queryStat, err := e.p.Perform(ctx)
				if err != nil {
					e.log().Error().Int("worker-id", workerID).Err(err).Msg("query error from worker")
				}
				e.queryChan <- queryStat
				pacing(start, e.cfg.Pacing)
				e.pgBar.Increment()
			}
		}
	}

	for i := 0; i < e.cfg.Workers; i++ {
		wg.Add(1)
		workerID := i
		go startWorker(&wg, workerID)
	}

	close(startSignal)
	return &wg
}

func (e *QuickExecutor) startWorkersOnIters(ctx context.Context) *sync.WaitGroup {
	var wg sync.WaitGroup
	startSignal := make(chan struct{})
	itersPerWorker := distributeIterations(e.cfg.Iterations, e.cfg.Workers)

	startWorker := func(wg *sync.WaitGroup, workerID, iterCount int) {
		defer wg.Done()
		if iterCount == 0 {
			return
		}
		<-startSignal
		for i := 0; i < iterCount; i++ {
			if ctx.Err() != nil {
				e.log().Error().Int("worker-id", workerID).Err(ctx.Err()).Msg("context err from worker")
				return
			}
			select {
			case <-ctx.Done():
				e.log().Info().Int("worker-id", workerID).Msg("getting 'Done' signal from the context")
				return
			default:
				start := time.Now()
				queryStat, err := e.p.Perform(ctx)
				if err != nil {
					e.log().Error().Int("worker-id", workerID).Err(err).Msg("query error from worker")
				}
				e.queryChan <- queryStat
				pacing(start, e.cfg.Pacing)
				e.pgBar.Increment()
			}
		}
	}

	for i := 0; i < e.cfg.Workers; i++ {
		wg.Add(1)
		workerID := i
		go startWorker(&wg, workerID, itersPerWorker[workerID])
	}

	close(startSignal)
	return &wg
}

func (e *QuickExecutor) start(ctx context.Context) *sync.WaitGroup {
	switch {
	case e.cfg.Iterations > 0:
		e.log().Info().Msg("starting the test based on iterations")
		return e.startWorkersOnIters(ctx)
	case e.cfg.Duration > 0:
		e.log().Info().Msg("starting the test based on duration")
		return e.startWorkersOnDur(ctx)
	default:
		e.log().Error().Msg("invalid configuration: both iterations and duration are zero")
		return nil
	}
}

func (e *QuickExecutor) log() *zerolog.Logger {
	return e.cfg.Logger
}

func getPreparedQuery(cfg *model.QuickRun) (*parse.PreparedQuery, error) {
	preparedQuery, err := parse.GetPreparedQuery(cfg.QueryTemplate)
	if err != nil {
		errMsg := "failed to get prepared query"
		cfg.Logger.Err(err).Msg(errMsg)
		return nil, fmt.Errorf("%s: %v", errMsg, err)
	}
	return preparedQuery, nil
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

func getProgressBar(cfg *model.QuickRun) (progress.ProgressReporter, error) {
	barCfg := progress.BarConfig{
		MaxValue:  getProgressBarMaxValue(cfg),
		EnableBar: true,
		Logger:    cfg.Logger,
	}
	return progress.NewProgressBarWrapper(barCfg)
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

func startStatsCollector(ctx context.Context, stats *stat.Stat, queryChan chan *stat.QueryStat) *sync.WaitGroup {
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
			case q, ok := <-queryChan:
				if !ok {
					return
				}
				stats.SubmitStat(q)
			}
		}
	}(&wg)

	return &wg
}

func getProgressBarMaxValue(cfg *model.QuickRun) int {
	switch {
	case cfg.Iterations > 0:
		return cfg.Iterations
	case cfg.Pacing > 0 && cfg.Duration > 0:
		return int(cfg.Duration/cfg.Pacing) * cfg.Workers
	default:
		return -1
	}
}
