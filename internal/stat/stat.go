/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package stat

import (
	"sync/atomic"
)

type Stat struct {
	success     atomic.Int64
	failed      atomic.Int64
	total       atomic.Int64
	min_latency atomic.Int64
	max_latency atomic.Int64
}

func (s *Stat) IncrementSuccess() {
	s.success.Add(1)
}

func (s *Stat) IncrementFailed() {
	s.failed.Add(1)
}

func (s *Stat) IncrementTotal() {
	s.total.Add(1)
}

func (s *Stat) SetMax(max int64) int64 {
	return s.max_latency.Swap(max)
}

func (s *Stat) SetMin(min int64) int64 {
	return s.min_latency.Swap(min)
}

func (s *Stat) SuccessValue() int {
	return int(s.success.Load())
}

func (s *Stat) FailedValue() int {
	return int(s.failed.Load())
}

func (s *Stat) TotalValue() int {
	return int(s.total.Load())
}

func (s *Stat) MaxValue() int64 {
	return s.max_latency.Load()
}

func (s *Stat) MinValue() int64 {
	return s.min_latency.Load()
}
