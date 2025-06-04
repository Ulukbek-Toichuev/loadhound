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
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal/db"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
)

type QuickRunErr struct {
	Message string
	Err     error
}

func NewQuickRunErr(msg string, err error) *QuickRunErr {
	return &QuickRunErr{msg, err}
}

func (q *QuickRunErr) Error() string {
	return q.Message
}

func (q *QuickRunErr) Unwrap() error {
	return q.Err
}

type QuickRun struct {
	Dsn        string
	Query      string
	Workers    int
	Iterations int
	Duration   time.Duration
	Pacing     time.Duration
	OutputFile string
}

func NewQuickRun(dsn, query string, workers, iterations int, duration, pacing time.Duration, outputFile string) *QuickRun {
	return &QuickRun{dsn, query, workers, iterations, duration, pacing, outputFile}
}

func QuickRunHandler(ctx context.Context, qr *QuickRun) error {
	err := validateQuickRunFields(qr)
	if err != nil {
		return err
	}

	st := runQuickTest(ctx, qr)
	if st != nil {
		stat.PrintSummary(st)
	}
	return nil
}

func validateQuickRunFields(qr *QuickRun) error {
	if qr.Dsn == "" {
		return NewQuickRunErr("--dsn is required", nil)
	}

	if qr.Query == "" {
		return NewQuickRunErr("--query is required", nil)
	}

	if qr.Workers < 0 {
		return NewQuickRunErr("--workers must be > 0", nil)
	}

	iterations := qr.Iterations
	duration := qr.Duration

	if iterations < 0 {
		return NewQuickRunErr("--iterations must be > 0", nil)
	}

	if duration < 0 {
		return NewQuickRunErr("--duration must be > 0", nil)
	}

	if iterations == 0 && duration == 0 {
		return NewQuickRunErr("either --iter or --duration must be set", nil)
	}

	if iterations > 0 && duration > 0 {
		return NewQuickRunErr("--iter and --duration are mutually exclusive", nil)
	}

	if qr.Pacing < 0 {
		return NewQuickRunErr("--pacing must be > 0", nil)
	}

	log.Println("fields of config are successfully validated")
	return nil
}

func runQuickTest(ctx context.Context, qr *QuickRun) *stat.Stat {
	connectionPool := db.GetPgxConn(ctx, qr.Dsn)
	log.Println("connection pool successfully init")

	if qr.Iterations != 0 && qr.Duration == 0 {
		log.Println("prepare test for execute by iterations")
		return execTestByIter(ctx, qr, connectionPool)
	}
	return nil
}

func execTestByIter(ctx context.Context, qr *QuickRun, connectionPool *db.CustomConnPgx) *stat.Stat {
	var st stat.Stat
	var wg sync.WaitGroup
	var itersByWorker int = qr.Iterations / qr.Workers
	startSignal := make(chan struct{})

	for i := 0; i < qr.Workers; i++ {
		wg.Add(1)
		workerId := i
		go func(st *stat.Stat, wg *sync.WaitGroup, workerId int) {
			<-startSignal
			defer wg.Done()
			for i := 0; i < itersByWorker; i++ {
				select {
				case <-ctx.Done():
					fmt.Printf("worker_id: %d: cancelled\n", workerId)
					return
				default:
					queryStat, err := connectionPool.QueryRowsWithLatency(ctx, qr.Query)
					stat.CollectStat(st, queryStat, err)
					stat.PrintCurrStat(workerId, i, queryStat)
				}
			}
		}(&st, &wg, workerId)
	}

	log.Printf("sent a start signal to workers\n\n")
	close(startSignal)
	wg.Wait()
	return &st
}
