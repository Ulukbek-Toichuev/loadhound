/*
LoadHound — Relentless load testing tool for SQL databases.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"fmt"
	"time"

	"github.com/caio/go-tdigest/v4"
)

const (
	tdigestCompression = 100.0
)

type ThreadStat struct {
	// TDigest for percentile calculations of response time
	td *tdigest.TDigest

	// Timing information
	startTime time.Time
	stopTime  time.Time

	iterationsTotal int64

	// Query result
	rowsAffected int64
	queriesTotal int64
	errorsTotal  int64

	// Error tracking
	errMap map[string]int64
}

func NewThreadStat() (*ThreadStat, error) {
	td, err := tdigest.New(tdigest.Compression(tdigestCompression))
	if err != nil {
		return nil, fmt.Errorf("failed to create TDigest: %w", err)
	}
	return &ThreadStat{
		td:     td,
		errMap: make(map[string]int64),
	}, nil
}

func (t *ThreadStat) SetStartTime(at time.Time) {
	t.startTime = at
}

func (t *ThreadStat) SetStopTime(at time.Time) {
	if !t.startTime.IsZero() && at.Before(t.startTime) {
		return
	}
	t.stopTime = at
}

func (t *ThreadStat) AddIter() {
	t.iterationsTotal++
}

func (t *ThreadStat) SubmitQueryResult(q *QueryResult) {
	if q == nil {
		return
	}
	t.rowsAffected += q.RowsAffected
	t.queriesTotal++
	if q.Err != nil {
		t.errorsTotal++
		t.errMap[q.Err.Error()]++
	}
	t.td.Add(float64(q.ResponseTime))
}

type GlobalMetric struct {
	StartAt           time.Time
	EndAt             time.Time
	Td                *tdigest.TDigest
	RowsAffectedTotal int64
	IterationsTotal   int64
	QueriesTotal      int64
	ErrorsTotal       int64
	ErrMap            map[string]int64
	Qps               float64
	ThreadsTotal      int
	threadStats       []*ThreadStat
}

func NewGlobalMetric(threadStats []*ThreadStat) *GlobalMetric {
	td, err := tdigest.New(tdigest.Compression(tdigestCompression))
	if err != nil {
		return nil
	}
	return &GlobalMetric{
		Td:          td,
		ErrMap:      make(map[string]int64),
		threadStats: threadStats,
	}
}

func (gm *GlobalMetric) Collect() {
	if gm.threadStats == nil {
		return
	}
	// Aggregate all metrics using snapshots for thread safety
	for _, threadStat := range gm.threadStats {
		if threadStat == nil {
			continue
		}

		// Merge TDigest
		gm.Td.Merge(threadStat.td)

		// Aggregate counters
		gm.RowsAffectedTotal += threadStat.rowsAffected
		gm.IterationsTotal += threadStat.iterationsTotal
		gm.QueriesTotal += threadStat.queriesTotal
		gm.ErrorsTotal += threadStat.errorsTotal

		// Aggregate error map
		for k, v := range threadStat.errMap {
			gm.ErrMap[k] += v
		}
	}

	// Calculate queries per second
	totalDuration := gm.EndAt.Sub(gm.StartAt)
	if totalDuration > 0 && gm.QueriesTotal > 0 {
		gm.Qps = float64(gm.QueriesTotal) / totalDuration.Seconds()
	}
}

func (gm *GlobalMetric) GetQPS() float64 {
	totalDuration := gm.EndAt.Sub(gm.StartAt).Seconds()
	if totalDuration <= 0 {
		return 0
	}
	return float64(gm.QueriesTotal) / totalDuration
}

func (gm *GlobalMetric) GetSuccessRate() float64 {
	if gm.QueriesTotal == 0 {
		return 0
	}
	successQueries := gm.QueriesTotal - gm.ErrorsTotal
	return (float64(successQueries) / float64(gm.QueriesTotal)) * 100
}

func (gm *GlobalMetric) GetFailedRate() float64 {
	if gm.QueriesTotal == 0 {
		return 0
	}
	return (float64(gm.ErrorsTotal) / float64(gm.QueriesTotal)) * 100
}
