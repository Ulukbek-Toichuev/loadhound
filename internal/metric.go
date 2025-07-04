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

	"github.com/influxdata/tdigest"
	"go.uber.org/atomic"
)

const (
	defaultRespMin = math.MaxInt64
	defaultRespMax = 0
)

type QueryMetric struct {
	Query        string
	ResponseTime time.Duration
	AffectedRows int64
	Err          error
	ThreadID     int
}

type MetricEngine struct {
	td            *TDigestWrapper
	start         atomic.Time
	curr          atomic.Time
	qps           atomic.Float64
	queryTotal    atomic.Int64
	iterTotal     atomic.Int64
	threadsTotal  atomic.Int64
	threadsFailed atomic.Int64
	errTotal      atomic.Int64
	respMin       atomic.Duration
	respMax       atomic.Duration
	respTotal     atomic.Duration
	errMap        sync.Map
}

func NewMetricEngine(compression float64) *MetricEngine {
	var m MetricEngine
	m.td = NewTDigestWrapper(compression)
	m.start.Store(time.Now())
	m.curr.Store(time.Now())
	m.respMin.Store(defaultRespMin)
	m.respMax.Store(defaultRespMax)
	return &m
}

func (m *MetricEngine) AddFailedThread() {
	m.threadsFailed.Add(1)
}

func (m *MetricEngine) AddThread() {
	m.threadsTotal.Add(1)
}

func (m *MetricEngine) AddIter() {
	m.iterTotal.Add(1)
}

func (m *MetricEngine) AddQueryMetric(q *QueryMetric) {
	m.queryTotal.Add(1)
	if q.Err != nil {
		m.errTotal.Add(1)
	}
	m.respTotal.Add(q.ResponseTime)

	for {
		respMinOld := m.respMin.Load()
		if q.ResponseTime >= respMinOld {
			break
		}
		if m.respMin.CompareAndSwap(respMinOld, q.ResponseTime) {
			break
		}
	}

	for {
		respMaxOld := m.respMax.Load()
		if q.ResponseTime <= respMaxOld {
			break
		}
		if m.respMax.CompareAndSwap(respMaxOld, q.ResponseTime) {
			break
		}
	}

	if q.Err != nil {
		for {
			actual, loaded := m.errMap.LoadOrStore(q.Err.Error(), 1)
			if !loaded {
				break
			}
			oldVal := actual.(int)
			newVal := oldVal + 1
			if m.errMap.CompareAndSwap(q.Err.Error(), oldVal, newVal) {
				break
			}
		}
	}

	now := time.Now()
	m.curr.Store(now)

	// Get QPS = TotalQueries / TotalDuration
	dur := now.Sub(m.start.Load())
	if dur.Seconds() > 0 {
		m.qps.Store(float64(m.queryTotal.Load()) / dur.Seconds())
	} else {
		m.qps.Store(0)
	}

	m.td.Add(float64(q.ResponseTime.Nanoseconds()))
}

type TDigestWrapper struct {
	td *tdigest.TDigest
	mu *sync.Mutex
	w  float64
}

func NewTDigestWrapper(compression float64) *TDigestWrapper {
	return &TDigestWrapper{
		td: tdigest.NewWithCompression(compression),
		mu: &sync.Mutex{},
		w:  1,
	}
}

func (tw *TDigestWrapper) Add(value float64) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	tw.td.Add(value, tw.w)
}
