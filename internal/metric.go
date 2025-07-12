/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"math"
	"sync"
	"time"

	"github.com/caio/go-tdigest/v4"
)

const (
	tdigestCompression = 100.0
	initialRespTimeMin = time.Duration(math.MaxInt64)
	initialRespTimeMax = time.Duration(0)
)

// Local metric for per thread
type LocalMetric struct {
	rmutex          *sync.RWMutex
	td              *tdigest.TDigest // Using TDigest data structure for percentiles
	startTime       time.Time        // Thread start time
	stopTime        time.Time        // Thread stop time
	rowsAffected    int64            // Rows affected total count
	iterationsTotal int64            // Iterations total count
	queriesTotal    int64            // Queries total count
	errorsTotal     int64            // Errors total count
	respTimeTotal   int64
	currRespTime    time.Duration
	minRespTime     time.Duration
	maxRespTime     time.Duration
	errMap          map[string]int // Map that contain error in string like key and his count like value int
}

// Constructor
func NewLocalMetric() (*LocalMetric, error) {
	td, err := tdigest.New(tdigest.Compression(tdigestCompression))
	if err != nil {
		return nil, err
	}
	return &LocalMetric{
		td:     td,
		errMap: make(map[string]int),
		rmutex: &sync.RWMutex{},
	}, nil
}

func (l *LocalMetric) StartAt(at time.Time) {
	l.rmutex.Lock()
	defer l.rmutex.Unlock()
	l.startTime = at
}

func (l *LocalMetric) AddIters() {
	l.rmutex.Lock()
	defer l.rmutex.Unlock()
	l.iterationsTotal++
}

func (l *LocalMetric) Submit(q *QueryResult) {
	l.rmutex.Lock()
	defer l.rmutex.Unlock()

	l.queriesTotal++
	l.rowsAffected += q.RowsAffected

	if q.Err != nil {
		l.errMap[q.Err.Error()]++
		l.errorsTotal++
	}
	l.respTimeTotal += int64(q.ResponseTime)
	l.currRespTime = q.ResponseTime

	if q.ResponseTime < l.minRespTime {
		l.minRespTime = q.ResponseTime
	}
	if q.ResponseTime > l.maxRespTime {
		l.maxRespTime = q.ResponseTime
	}
	l.td.Add(float64(q.ResponseTime))
}

func (l *LocalMetric) Stop() {
	l.rmutex.Lock()
	defer l.rmutex.Unlock()
	l.stopTime = time.Now()
}

func (l *LocalMetric) Clone() *LocalMetric {
	l.rmutex.RLock()
	defer l.rmutex.RUnlock()

	tdCopy := l.td.Clone()

	errMap := make(map[string]int, len(l.errMap))
	for k, v := range l.errMap {
		errMap[k] = v
	}

	return &LocalMetric{
		rmutex:          &sync.RWMutex{},
		td:              tdCopy,
		startTime:       l.startTime,
		stopTime:        l.stopTime,
		rowsAffected:    l.rowsAffected,
		iterationsTotal: l.iterationsTotal,
		queriesTotal:    l.queriesTotal,
		errorsTotal:     l.errorsTotal,
		respTimeTotal:   l.respTimeTotal,
		errMap:          errMap,
	}
}

type GlobalMetric struct {
	Td                *tdigest.TDigest
	RowsAffectedTotal int64
	IterationsTotal   int64
	QueriesTotal      int64
	ErrorsTotal       int64
	ErrMap            map[string]int
	RespTime          *ResponseTime
	Qps               *QueryPerSecond
}

type ResponseTime struct {
	Total int64
	Min   time.Duration
	Max   time.Duration
	Avg   time.Duration
}

type QueryPerSecond struct {
	Total float64
	Min   float64
	Max   float64
	Avg   float64
}

func NewGlobalMetric() *GlobalMetric {
	td, err := tdigest.New(tdigest.Compression(tdigestCompression))
	if err != nil {
		return nil
	}
	return &GlobalMetric{
		Td: td,
		RespTime: &ResponseTime{
			Min: initialRespTimeMin,
			Max: initialRespTimeMax,
		},
		Qps:    &QueryPerSecond{},
		ErrMap: make(map[string]int),
	}
}

func (gm *GlobalMetric) Collect(threadMetrics []*LocalMetric) {
	for _, tm := range threadMetrics {
		// Merge TDigest
		gm.Td.Merge(tm.td)

		gm.RowsAffectedTotal += tm.rowsAffected
		gm.IterationsTotal += tm.iterationsTotal
		gm.QueriesTotal += tm.queriesTotal
		gm.ErrorsTotal += tm.errorsTotal
		for k, v := range tm.errMap {
			gm.ErrMap[k] += v
		}

		// Update ResponseTime
		if tm.maxRespTime > gm.RespTime.Max {
			gm.RespTime.Max = tm.maxRespTime
		}

		if tm.minRespTime < gm.RespTime.Min {
			gm.RespTime.Min = tm.minRespTime
		}
		gm.RespTime.Avg = time.Duration(gm.RespTime.Total / gm.QueriesTotal)

		// Update Query per second
		testDuration := time.Since(tm.startTime)
		if testDuration > 0 {
			qps := float64(gm.QueriesTotal) / testDuration.Seconds()
			gm.Qps.Total += qps
			if qps > gm.Qps.Max {
				gm.Qps.Max = qps
			}
			if qps < gm.Qps.Min {
				gm.Qps.Min = qps
			}
		}
		if gm.QueriesTotal > 0 {
			gm.RespTime.Avg = time.Duration(gm.RespTime.Total / gm.QueriesTotal)
			gm.Qps.Avg = gm.Qps.Total / float64(gm.QueriesTotal)
		}

	}
}
