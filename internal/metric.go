/*
LoadHound — Relentless load testing tool for SQL-oriented RDBMS.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"math"
	"sync"
	"sync/atomic"
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
	// TDigest for percentile calculations
	td *tdigest.TDigest

	// Timing information
	startTime time.Time
	stopTime  time.Time

	// Atomic counters for high-frequency operations
	rowsAffected    int64 // atomic
	iterationsTotal int64 // atomic
	queriesTotal    int64 // atomic
	errorsTotal     int64 // atomic
	respTimeTotal   int64 // atomic (in nanoseconds)

	// Response time tracking
	currRespTime time.Duration
	minRespTime  time.Duration
	maxRespTime  time.Duration

	// Error tracking
	errMap map[string]int64
}

// Constructor
func NewLocalMetric() (*LocalMetric, error) {
	td, err := tdigest.New(tdigest.Compression(tdigestCompression))
	if err != nil {
		return nil, err
	}
	return &LocalMetric{
		td:          td,
		errMap:      make(map[string]int64),
		minRespTime: initialRespTimeMin,
		maxRespTime: initialRespTimeMax,
	}, nil
}

func (l *LocalMetric) StartAt(at time.Time) {
	l.startTime = at
}

func (l *LocalMetric) AddIters() {
	l.iterationsTotal++
}

func (l *LocalMetric) Submit(q *QueryResult) {
	if q == nil {
		return
	}

	// Update atomic counters
	atomic.AddInt64(&l.rowsAffected, q.RowsAffected)
	atomic.AddInt64(&l.queriesTotal, 1)

	// Handle errors
	if q.Err != nil {
		atomic.AddInt64(&l.errorsTotal, 1)
		l.errMap[q.Err.Error()]++
		return
	}

	respTimeNs := int64(q.ResponseTime)
	atomic.AddInt64(&l.respTimeTotal, respTimeNs)

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
	l.stopTime = time.Now()
}

type GlobalMetric struct {
	startTime         time.Time
	mutex             *sync.RWMutex
	Td                *tdigest.TDigest
	RowsAffectedTotal int64
	IterationsTotal   int64
	QueriesTotal      int64
	ErrorsTotal       int64
	ErrMap            map[string]int64
	RespTime          *ResponseTime
	Qps               *QueryPerSecond
	ThreadsTotal      int
}

type ResponseTime struct {
	Total int64
	Min   time.Duration
	Max   time.Duration
	Avg   time.Duration
}

type QueryPerSecond struct {
	Current float64
	Peak    float64
}

func NewGlobalMetric() *GlobalMetric {
	td, err := tdigest.New(tdigest.Compression(tdigestCompression))
	if err != nil {
		return nil
	}
	return &GlobalMetric{
		startTime: time.Now(),
		Td:        td,
		mutex:     &sync.RWMutex{},
		RespTime: &ResponseTime{
			Min: initialRespTimeMin,
			Max: initialRespTimeMax,
		},
		Qps:    &QueryPerSecond{},
		ErrMap: make(map[string]int64),
	}
}

func (gm *GlobalMetric) SetThreadCount(t int) {
	gm.ThreadsTotal += t
}

func (gm *GlobalMetric) GetStartTime() time.Time {
	return gm.startTime
}

func (gm *GlobalMetric) Collect(threadMetrics []*LocalMetric) {
	if threadMetrics == nil {
		return
	}

	// Reset aggregated values
	gm.RowsAffectedTotal = 0
	gm.IterationsTotal = 0
	gm.QueriesTotal = 0
	gm.ErrorsTotal = 0
	gm.RespTime.Total = 0
	gm.RespTime.Min = initialRespTimeMin
	gm.RespTime.Max = initialRespTimeMax
	gm.ErrMap = make(map[string]int64)

	// Create a new TDigest for fresh calculation
	td, err := tdigest.New(tdigest.Compression(tdigestCompression))
	if err != nil {
		return
	}
	gm.Td = td

	// Find earliest start time and latest stop time
	var earliestStart, latestStop time.Time
	for i, tm := range threadMetrics {
		if tm == nil {
			continue
		}

		if i == 0 || tm.startTime.Before(earliestStart) {
			earliestStart = tm.startTime
		}
		if tm.stopTime.After(latestStop) {
			latestStop = tm.stopTime
		}
	}

	// Aggregate all metrics
	for _, tm := range threadMetrics {
		if tm == nil {
			continue
		}

		// Merge TDigest
		gm.Td.Merge(tm.td)

		// Aggregate counters
		gm.RowsAffectedTotal += tm.rowsAffected
		gm.IterationsTotal += tm.iterationsTotal
		gm.QueriesTotal += tm.queriesTotal
		gm.ErrorsTotal += tm.errorsTotal
		gm.RespTime.Total += tm.respTimeTotal

		// Aggregate error map
		for k, v := range tm.errMap {
			gm.ErrMap[k] += v
		}

		// Update min/max response times
		if tm.minRespTime < gm.RespTime.Min {
			gm.RespTime.Min = tm.minRespTime
		}
		if tm.maxRespTime > gm.RespTime.Max {
			gm.RespTime.Max = tm.maxRespTime
		}
	}

	// Calculate average response time
	if gm.QueriesTotal > 0 {
		gm.RespTime.Avg = time.Duration(gm.RespTime.Total / gm.QueriesTotal)
	}

	// Calculate QPS based on total test duration
	testDuration := latestStop.Sub(earliestStart)
	if testDuration > 0 && gm.QueriesTotal > 0 {
		gm.Qps.Current = float64(gm.QueriesTotal) / testDuration.Seconds()
		gm.Qps.Peak = gm.Qps.Current // For final report, current and peak are the same
	}
}
