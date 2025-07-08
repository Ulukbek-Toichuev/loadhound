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
	defCompression float64 = 1000
	defaultRespMin         = math.MaxInt64
	defaultRespMax         = 0
)

// Local metric for per thread
type LocalMetric struct {
	td                *tdigest.TDigest // Using TDigest data structure for percentiles
	start             time.Time        // Thread start time
	rowsAffectedTotal int64            // Rows affected total count
	iterationsTotal   int64            // Iterations total count
	queriesTotal      int64            // Queries total count
	errorsTotal       int64            // Errors total count
	respTimeTotal     int64
	respMax           time.Duration  // Max response time in current moment
	respMin           time.Duration  // Min response time in current moment
	respAvg           time.Duration  // Averarge response time in current monet
	qps               float64        // Queries per second based on thread start time and current metric submited time
	errMap            map[string]int // Map that contain error in string like key and his count like value int
	rm                *sync.RWMutex
}

// Constructor
func NewLocalMetric() (*LocalMetric, error) {
	td, err := tdigest.New(tdigest.Compression(defCompression))
	if err != nil {
		return nil, err
	}
	return &LocalMetric{
		td:      td,
		start:   time.Now(),
		respMin: defaultRespMin,
		respMax: defaultRespMax,
		errMap:  make(map[string]int),
		rm:      &sync.RWMutex{},
	}, nil
}

func (l *LocalMetric) SetStartTIme() {
	l.start = time.Now()
}

func (l *LocalMetric) SetQueryMetric(q *QueryMetric) {
	l.rm.RLock()
	defer l.rm.RUnlock()

	l.queriesTotal++
	l.rowsAffectedTotal += q.RowsAffected

	if q.ResponseTime < l.respMin {
		l.respMin = q.ResponseTime
	}

	if q.ResponseTime > l.respMax {
		l.respMax = q.ResponseTime
	}

	if q.Err != nil {
		l.errMap[q.Err.Error()]++
		l.errorsTotal++
	}
	l.respTimeTotal += int64(q.ResponseTime)

	l.respAvg = time.Duration(l.respTimeTotal / l.queriesTotal)

	testDuration := time.Since(l.start)
	if testDuration > 0 {
		l.qps = float64(l.queriesTotal) / testDuration.Seconds()
	}
	l.td.Add(float64(q.ResponseTime))
}

type QueryMetric struct {
	Query        string
	ResponseTime time.Duration
	RowsAffected int64
	Err          error
	ThreadId     int
}

type MetricEngine struct {
}
