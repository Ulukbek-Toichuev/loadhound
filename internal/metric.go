/*
LoadHound — Relentless load testing tool for SQL databases.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"fmt"
	"sync"
	"time"

	"github.com/caio/go-tdigest/v4"
)

const tdigestCompression = 100.0

type Metric struct {
	mu *sync.Mutex

	// TDigest for percentile calculations of response time
	Td *tdigest.TDigest

	// Timing information
	StartTime time.Time
	StopTime  time.Time

	IterationsTotal int64
	ThreadsTotal    int64

	// Query result
	RowsAffected int64
	QueriesTotal int64
	ErrorsTotal  int64

	// Error tracking
	ErrMap map[string]int64
}

func NewMetric() (*Metric, error) {
	td, err := tdigest.New(tdigest.Compression(tdigestCompression))
	if err != nil {
		return nil, fmt.Errorf("failed to create TDigest: %w", err)
	}
	return &Metric{
		mu:     &sync.Mutex{},
		Td:     td,
		ErrMap: make(map[string]int64),
	}, nil
}

func (m *Metric) SetStartTime(at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.StartTime = at
}

func (m *Metric) SetStopTime(at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.StartTime.IsZero() && at.Before(m.StartTime) {
		return
	}
	m.StopTime = at
}

func (m *Metric) AddIter() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.IterationsTotal++
}

func (m *Metric) AddThread() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ThreadsTotal++
}

func (m *Metric) SubmitQueryResult(q *QueryResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if q == nil {
		return nil
	}

	m.RowsAffected += q.RowsAffected
	m.QueriesTotal++
	if q.Err != nil {
		m.ErrorsTotal++
		m.ErrMap[q.Err.Error()]++
	}
	return m.Td.Add(float64(q.ResponseTime))
}

func (m *Metric) GetSnapshot() *Metric {
	m.mu.Lock()
	defer m.mu.Unlock()

	tdCopy := m.Td.Clone()

	errMapCopy := make(map[string]int64, len(m.ErrMap))
	for k, v := range m.ErrMap {
		errMapCopy[k] = v
	}

	return &Metric{
		StartTime:       m.StartTime,
		StopTime:        m.StopTime,
		IterationsTotal: m.IterationsTotal,
		RowsAffected:    m.RowsAffected,
		QueriesTotal:    m.QueriesTotal,
		ErrorsTotal:     m.ErrorsTotal,
		Td:              tdCopy,
		ErrMap:          errMapCopy,
	}
}

func (m *Metric) GetQPS() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	totalDuration := m.StopTime.Sub(m.StartTime).Seconds()
	if totalDuration <= 0 {
		return 0
	}
	return float64(m.QueriesTotal) / totalDuration
}

func (m *Metric) GetSuccessRate() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.QueriesTotal == 0 {
		return 0
	}
	successQueries := m.QueriesTotal - m.ErrorsTotal
	return (float64(successQueries) / float64(m.QueriesTotal)) * 100
}

func (m *Metric) GetFailedRate() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.QueriesTotal == 0 {
		return 0
	}
	return (float64(m.ErrorsTotal) / float64(m.QueriesTotal)) * 100
}
