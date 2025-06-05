/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package stat

import (
	"sync"
	"time"
)

type QueryStat struct {
	Latency      time.Duration
	Err          error
	AffectedRows int64
}

func NewQueryStat(latency time.Duration, err error, affectedRows int64) *QueryStat {
	return &QueryStat{latency, err, affectedRows}
}

type Stat struct {
	mu          sync.Mutex
	Success     int
	Failed      int
	Total       int
	Min_latency time.Duration
	Max_latency time.Duration
}

func NewStat() *Stat {
	return &Stat{mu: sync.Mutex{}}
}

func (s *Stat) SubmitStat(qr *QueryStat) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if qr.Err != nil {
		s.Failed++
	} else {
		s.Success++
	}

	if qr.Latency < s.Min_latency && s.Min_latency != 0 {
		s.Min_latency = qr.Latency
	} else if qr.Latency > s.Max_latency {
		s.Max_latency = qr.Latency
	}

	if s.Min_latency == 0 {
		s.Min_latency = qr.Latency
	}

	s.Total++
}

type SummaryStat struct {
	TestStart    time.Time
	TestEnd      time.Time
	TotalStat    *Stat
	WorkersCount int
	Iterations   int
}

func NewSummaryStat(totalStat *Stat, testStart, testEnd time.Time, workersCount, iterations int) *SummaryStat {
	return &SummaryStat{testStart, testEnd, totalStat, workersCount, iterations}
}
