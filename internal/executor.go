/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"context"
	"fmt"
	"log"
	"sync"
	"text/template"
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal/db"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"
)

type QueryRunner interface {
	Run(ctx context.Context) (*stat.QueryStat, error)
}

type LoadTestExecutor struct {
	cfg       *QuickRun
	conn      *db.CustomConnPgx
	tmpl      *template.Template
	queryChan chan *stat.QueryStat
	queryFunc QueryRunner
}

func NewLoadTestExecutor(ctx context.Context, cfg *QuickRun, tmpl *template.Template) (*LoadTestExecutor, error) {
	conn := db.GetPgxConn(ctx, cfg.Dsn)

	preparedQuery, err := GetPrepareQuery(cfg.Query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare query: %v", err)
	}
	cfg.Query = preparedQuery.RawSQL

	var runner QueryRunner
	switch preparedQuery.QueryType {
	case QueryTypeRead:
		runner = NewQueryReader(conn, tmpl)
	case QueryTypeExec:
		runner = NewQueryExecutor(conn, tmpl)
	default:
		return nil, fmt.Errorf("unsupported query type: %s", preparedQuery.QueryType)
	}

	return &LoadTestExecutor{
		cfg:       cfg,
		conn:      conn,
		tmpl:      tmpl,
		queryChan: make(chan *stat.QueryStat, 10*cfg.Workers),
		queryFunc: runner,
	}, nil
}

func (e *LoadTestExecutor) startStatsCollector(ctx context.Context, stats *stat.Stat) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			case q, ok := <-e.queryChan:
				if !ok {
					return
				}
				stats.SubmitStat(q)
			}
		}
	}(&wg)

	return &wg
}

func (e *LoadTestExecutor) startWorkers(ctx context.Context) *sync.WaitGroup {
	var wg sync.WaitGroup
	startSignal := make(chan struct{})
	iterations := distributeIterations(e.cfg.Iterations, e.cfg.Workers)

	startWorker := func(wg *sync.WaitGroup, workerID, iterCount int, start chan struct{}) {
		defer wg.Done()
		<-start
		for i := 0; i < iterCount; i++ {
			if ctx.Err() != nil {
				log.Printf("[worker id: %d] cancelled", workerID)
				return
			}

			start := time.Now()
			statEntry, err := e.queryFunc.Run(ctx)
			pacing(start, e.cfg.Pacing)
			if err != nil {
				log.Printf("[worker id: %d] query error: %v", workerID, err)
				return
			}

			select {
			case <-ctx.Done():
				log.Printf("[worker id: %d] get signal from context", workerID)
				return
			default:
				e.queryChan <- statEntry
			}
		}
	}

	for i := 0; i < e.cfg.Workers; i++ {
		wg.Add(1)

		workerID := i
		go startWorker(&wg, workerID, iterations[workerID], startSignal)
	}

	close(startSignal)
	return &wg
}

func (e *LoadTestExecutor) Run(ctx context.Context) *stat.Stat {
	stats := stat.NewStat()
	wgStats := e.startStatsCollector(ctx, stats)

	wgWorkers := e.startWorkers(ctx)
	pkg.LogWrapper("Starting load test by iterations...")

	wgWorkers.Wait()
	close(e.queryChan)

	wgStats.Wait()
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
