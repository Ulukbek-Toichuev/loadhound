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
	"github.com/Ulukbek-Toichuev/loadhound/internal/parse"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
	"github.com/schollz/progressbar/v3"

	"github.com/rs/zerolog"
)

type Executor interface {
	Run(ctx context.Context) (*stat.QueryStat, error)
}

type ExecutorError struct {
	Message string
	Err     error
}

func NewExecutorError(msg string, err error) *ExecutorError {
	return &ExecutorError{msg, err}
}

func (e *ExecutorError) Error() string {
	return e.Message
}

func (e *ExecutorError) Unwrap() error {
	return e.Err
}

type QuickExecutor struct {
	cfg       *QuickRun
	conn      *db.CustomConnPgx
	tmpl      *template.Template
	queryChan chan *stat.QueryStat
	queryFunc Executor
}

func NewQuickExecutor(ctx context.Context, cfg *QuickRun, tmpl *template.Template) (*QuickExecutor, error) {
	conn := db.GetPgxConn(ctx, cfg.Dsn)

	preparedQuery, err := parse.GetPreparedQuery(cfg.Query)
	if err != nil {
		errMsg := "failed to get prepared query"
		cfg.Logger.Err(err).Msg(errMsg)
		return nil, NewExecutorError(errMsg, err)
	}
	cfg.Query = preparedQuery.RawSQL

	var runner Executor
	switch preparedQuery.QueryType {
	case parse.QueryTypeRead:
		runner = NewQueryReader(conn, tmpl)
	case parse.QueryTypeExec:
		runner = NewQueryExecutor(conn, tmpl)
	default:
		errMsg := fmt.Sprintf("unsupported query type: %s", preparedQuery.QueryType)
		cfg.Logger.Error().Msg(errMsg)
		return nil, NewExecutorError(errMsg, nil)
	}

	return &QuickExecutor{
		cfg:       cfg,
		conn:      conn,
		tmpl:      tmpl,
		queryChan: make(chan *stat.QueryStat, 10*cfg.Workers),
		queryFunc: runner,
	}, nil
}

func (e *QuickExecutor) startWorkers(ctx context.Context) (*sync.WaitGroup, *progressbar.ProgressBar) {
	var wg sync.WaitGroup
	startSignal := make(chan struct{})
	itersPerWorker := distributeIterations(e.cfg.Iterations, e.cfg.Workers)

	e.cfg.Logger.Info().Msg("init progress bar")
	pgBar := initProgress(e.cfg.Iterations)

	startWorker := func(wg *sync.WaitGroup, workerID, iterCount int, start chan struct{}, logger *zerolog.Logger) {
		defer wg.Done()
		if iterCount == 0 {
			return
		}
		<-start
		for i := 0; i < iterCount; i++ {
			if ctx.Err() != nil {
				logger.Error().Int("worker-id", workerID).Err(ctx.Err()).Msg("context err from worker")
				return
			}

			start := time.Now()
			queryStat, err := e.queryFunc.Run(ctx)
			if err != nil {
				logger.Error().Int("worker-id", workerID).Err(err).Msg("query error from worker")
				continue
			}

			select {
			case <-ctx.Done():
				logger.Info().Int("worker-id", workerID).Msg("getting 'Done' signal from the context")
				return
			default:
				e.queryChan <- queryStat
				pgBar.Add(1)
			}
			pacing(start, e.cfg.Pacing)
		}
	}

	for i := 0; i < e.cfg.Workers; i++ {
		wg.Add(1)

		workerID := i
		go startWorker(&wg, workerID, itersPerWorker[workerID], startSignal, e.cfg.Logger)
	}

	close(startSignal)
	return &wg, pgBar
}

func (e *QuickExecutor) Run(ctx context.Context) *stat.Stat {
	stats := stat.NewStat()
	wgStats := startStatsCollector(ctx, stats, e.queryChan)

	wgWorkers, pgBar := e.startWorkers(ctx)
	e.cfg.Logger.Info().Msg("starting the test based on iterations")

	wgWorkers.Wait()
	close(e.queryChan)

	wgStats.Wait()
	pgBar.Finish()
	fmt.Printf("\n")
	return stats
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

func initProgress(maxValue int) *progressbar.ProgressBar {
	bar := progressbar.NewOptions(maxValue,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowIts(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("Running..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	return bar
}
