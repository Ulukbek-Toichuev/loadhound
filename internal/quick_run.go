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
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal/db"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"

	"github.com/google/uuid"
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

	tmpl := template.New(qr.Query)
	tmpl, err = tmpl.Parse(qr.Query)
	if err != nil {
		pkg.PrintFatal("failed to parse your query", err)
	}

	startTestTime := time.Now()
	st := runTest(ctx, qr, tmpl)
	if st != nil {
		stat.PrintSummary(stat.NewSummaryStat(st, startTestTime, time.Now(), qr.Workers, qr.Iterations))
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

func runTest(ctx context.Context, qr *QuickRun, tmpl *template.Template) *stat.Stat {
	connectionPool := db.GetPgxConn(ctx, qr.Dsn)
	log.Println("connection pool successfully init")

	log.Println("prepare worker for collect stats")
	var wg sync.WaitGroup
	wg.Add(1)

	queryChan := make(chan *stat.QueryStat, qr.Workers*2)

	globalStat := stat.NewStat()
	go func(wg *sync.WaitGroup, gs *stat.Stat) {
		defer wg.Done()
		for st := range queryChan {
			gs.SubmitStat(st)
		}
	}(&wg, globalStat)

	if qr.Iterations != 0 && qr.Duration == 0 {
		log.Println("prepare test for execute by iterations")
		runTestByIterations(ctx, qr, connectionPool, queryChan, tmpl)
	}

	close(queryChan)
	wg.Wait()
	return globalStat
}

func runTestByIterations(ctx context.Context, qr *QuickRun, connectionPool *db.CustomConnPgx, queryChan chan *stat.QueryStat, tmpl *template.Template) {
	var wg sync.WaitGroup
	itersPerWorker := make([]int, qr.Workers)
	for i := 0; i < qr.Iterations; i++ {
		itersPerWorker[i%qr.Workers]++
	}
	startSignal := make(chan struct{})

	for i := 0; i < qr.Workers; i++ {
		wg.Add(1)
		workerID := i
		iters := itersPerWorker[i]
		go func(wg *sync.WaitGroup, workerID int, iters int) {
			<-startSignal
			defer wg.Done()
			for i := 0; i < iters; i++ {
				select {
				case <-ctx.Done():
					fmt.Printf("worker_id: %d: cancelled\n", workerID)
					return
				default:
					queryStat, err := execQuery(ctx, connectionPool, tmpl)
					if err != nil {
						log.Printf("worker_id: %d query failed: %v\n", workerID, err)
						continue
					}
					select {
					case <-ctx.Done():
						return
					case queryChan <- queryStat:
					}
				}
			}
		}(&wg, workerID, iters)
	}

	log.Printf("sent a start signal to workers\n\n")
	close(startSignal)
	wg.Wait()
}

func execQuery(ctx context.Context, connectionPool *db.CustomConnPgx, tmpl *template.Template) (*stat.QueryStat, error) {
	preparedQuery := buildStmt(tmpl)
	execType := getExecType(preparedQuery)
	if execType == "" {
		return nil, fmt.Errorf("unsupported query type: %s", preparedQuery)
	}

	var queryStat *stat.QueryStat
	var e error
	if execType == "query" {
		q, err := connectionPool.QueryRowsWithLatency(ctx, preparedQuery)
		queryStat = q
		e = err
	} else if execType == "exec" {
		q, err := connectionPool.ExecWithLatency(ctx, preparedQuery)
		queryStat = q
		e = err
	}
	return queryStat, e
}

func getExecType(query string) string {
	query = strings.ToUpper(query)
	if strings.HasPrefix(query, "INSERT") || strings.HasPrefix(query, "UPDATE") || strings.HasPrefix(query, "DELETE") {
		return "exec"
	} else if strings.HasPrefix(query, "SELECT") {
		return "query"
	}

	return ""
}

func buildStmt(t *template.Template) string {
	sb := &strings.Builder{}

	data := struct {
		RandIntRange       func(min, max int) int
		RandFloat64InRange func(min, max float64) float64
		RandUUID           func() *uuid.UUID
		RandStringInRange  func(min, max int) string
		GetTime            func() string
	}{
		RandIntRange:       pkg.RandIntRange,
		RandFloat64InRange: pkg.RandFloat64InRange,
		RandUUID:           pkg.RandUUID,
		RandStringInRange:  pkg.RandStringInRange,
		GetTime:            pkg.GetTime,
	}
	if err := t.Execute(sb, data); err != nil {
		pkg.PrintFatal("failed to execute template", err)
	}
	return sb.String()
}
