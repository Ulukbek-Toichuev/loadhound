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

	"github.com/Ulukbek-Toichuev/loadhound/internal/db"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"
)

func ExecuteQuickTest(ctx context.Context, qr *QuickRun, tmpl *template.Template) *stat.Stat {
	pkg.LogWrapper("Initializing connection pool...")
	connectionPool := db.GetPgxConn(ctx, qr.Dsn)

	queryChan := make(chan *stat.QueryStat, qr.Workers*2)
	globalStat := stat.NewStat()

	var wg sync.WaitGroup
	wg.Add(1)
	go processStat(ctx, &wg, globalStat, queryChan)

	pkg.LogWrapper("Building execution plan...")
	exec := NewQExecutor(qr, connectionPool, queryChan, tmpl)
	if qr.Iterations != 0 && qr.Duration == 0 {
		exec.RunTestByIterations(ctx)
	}

	close(queryChan)
	wg.Wait()
	return globalStat
}

func processStat(ctx context.Context, wg *sync.WaitGroup, gs *stat.Stat, queryChan chan *stat.QueryStat) {
	defer wg.Done()
	pkg.LogWrapper("Initializing stats submit process")
	for {
		select {
		case <-ctx.Done():
			log.Println("finish stats submit process")
			return
		case st, ok := <-queryChan:
			if !ok {
				return
			}
			gs.SubmitStat(st)
		}
	}
}

type QExecutor struct {
	qr        *QuickRun
	queryType string
	connPool  *db.CustomConnPgx
	queryChan chan *stat.QueryStat
	tmpl      *template.Template
}

func NewQExecutor(qr *QuickRun, connPool *db.CustomConnPgx, queryChan chan *stat.QueryStat, tmpl *template.Template) *QExecutor {
	typeStr := GetQueryType(qr.Query)
	if typeStr == "" {
		pkg.PrintFatal(fmt.Sprintf("unsupported query type: %s", qr.Query), nil)
	}
	return &QExecutor{
		qr:        qr,
		queryType: typeStr,
		connPool:  connPool,
		queryChan: queryChan,
		tmpl:      tmpl,
	}
}

func (q *QExecutor) RunTestByIterations(ctx context.Context) {
	var wg sync.WaitGroup
	itersPerWorker := getItersPerWorker(q.qr)
	startSignal := make(chan struct{})

	for i := 0; i < q.qr.Workers; i++ {
		wg.Add(1)
		workerID := i

		iters := itersPerWorker[i]
		go q.runWorker(ctx, workerID, iters, startSignal, &wg)
	}
	pkg.LogWrapper("Executing quick test by iterations")
	close(startSignal)
	wg.Wait()
}

func (q *QExecutor) runWorker(ctx context.Context, workerID, iters int, startSignal <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	<-startSignal
	for i := 0; i < iters; i++ {
		if ctx.Err() != nil {
			log.Printf("worker_id: %d cancelled", workerID)
			return
		}
		queryStat, err := q.execQuery(ctx)
		if err != nil {
			log.Printf("worker_id: %d query failed: %v\n", workerID, err)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case q.queryChan <- queryStat:
		}
	}
}

func (q *QExecutor) execQuery(ctx context.Context) (*stat.QueryStat, error) {
	preparedQuery, err := BuildStmt(q.tmpl)
	if err != nil {
		return nil, fmt.Errorf("failed to build statement from template: %v", err)
	}
	switch q.queryType {
	case "query":
		return q.connPool.QueryRowsWithLatency(ctx, preparedQuery)
	case "exec":
		return q.connPool.ExecWithLatency(ctx, preparedQuery)
	default:
		return nil, fmt.Errorf("unsupported query type: %s", q.queryType)
	}
}

func getItersPerWorker(qr *QuickRun) []int {
	itersPerWorker := make([]int, qr.Workers)
	for i := 0; i < qr.Iterations; i++ {
		itersPerWorker[i%qr.Workers]++
	}
	return itersPerWorker
}
