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

	runQuickTest(ctx, qr)
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

	return nil
}

func runQuickTest(ctx context.Context, qr *QuickRun) error {
	cn := GetPgxConn(context.Background(), qr.Dsn)

	perWorker := qr.Iterations / qr.Workers

	execQuery := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		rows, dur, err := cn.QueryRowsWithLatency(ctx, qr.Query)
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Printf("rows count: %d -- duration: %d ms\n", rows, dur)
	}

	var wg sync.WaitGroup
	for i := 0; i < qr.Workers; i++ {
		wg.Add(1)
		select {
		case <-ctx.Done():
			fmt.Println("get signal from gracefull shutdown")
			return nil
		default:
			go func(wg *sync.WaitGroup, workerId int) {
				defer wg.Done()
				for i := 0; i < perWorker; i++ {
					select {
					case <-ctx.Done():
						fmt.Printf("get signal from gracefull shutdown, worker id: %d\n", i)
						return
					default:
						execQuery()
					}
				}
			}(&wg, i)
		}
	}

	wg.Wait()
	return nil
}
