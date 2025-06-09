/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package stat

import (
	"math"
	"sort"
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
	mu              sync.Mutex
	Success         int
	Failed          int
	Total           int
	QueriesRespList []int64
	ErrMap          map[string]int
}

func NewStat() *Stat {
	return &Stat{mu: sync.Mutex{}, QueriesRespList: make([]int64, 0), ErrMap: make(map[string]int)}
}

func (s *Stat) SubmitStat(qr *QueryStat) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if qr.Err != nil {
		s.Failed++
		s.ErrMap[qr.Err.Error()]++
	} else {
		s.Success++
		s.QueriesRespList = append(s.QueriesRespList, qr.Latency.Milliseconds())
	}

	s.Total++
}

type Result struct {
	Start          time.Time `json:"start"`
	End            time.Time `json:"end"`
	TotalTime      time.Time `json:"total_time"`
	TotalQueries   int       `json:"total_queries"`
	SuccessQueries int       `json:"successfull_queries"`
	FailedQueries  int       `json:"failed_queries"`
	Throughput     float64   `json:"throuphput"`
	Latency        *Latency  `json:"latency"`
	TopErrors      []string  `json:"top_errors"`
}

type Latency struct {
	Min    int64   `json:"min"`
	Max    int64   `json:"max"`
	Avg    float64 `json:"avg"`
	Median int64   `json:"median"`
	P90    int64   `json:"p90"`
	P95    int64   `json:"p95"`
	P99    int64   `json:"p99"`
}

func NewLatency(min, max, median, p90, p95, p99 int64, avg float64) *Latency {
	return &Latency{min, max, avg, median, p90, p95, p99}
}

func GetResult(start, end time.Time, total time.Duration, stat *Stat) *Result {
	var result Result
	result.Start = start
	result.End = end
	result.TotalQueries = stat.Total
	result.SuccessQueries = stat.Success
	result.FailedQueries = stat.Failed
	result.Latency = GetLatency(stat.QueriesRespList)
	result.Throughput = throughput(len(stat.QueriesRespList), total)
	return &result
}

func GetLatency(data []int64) *Latency {
	var latency Latency
	if len(data) == 0 {
		return &latency
	}

	sorted := make([]int64, len(data))
	copy(sorted, data)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	min, max := minMax(sorted)
	avg := average(sorted)
	md := median(sorted)
	p90 := percentile(sorted, 90)
	p95 := percentile(sorted, 95)
	p99 := percentile(sorted, 99)
	return NewLatency(min, max, md, p90, p95, p99, avg)
}

func minMax(data []int64) (int64, int64) {
	return data[0], data[len(data)-1]
}

func average(data []int64) float64 {
	var sum int64
	for _, d := range data {
		sum += d
	}
	return float64(sum / int64(len(data)))
}

func median(data []int64) int64 {
	n := len(data)
	if n%2 == 1 {
		return data[n/2]
	} else {
		md1 := data[n/2-1]
		md2 := data[n/2]
		md := (md1 + md2) / 2.0
		return md
	}
}

func percentile(data []int64, p float64) int64 {
	n := len(data)
	index := int(math.Ceil(p/100*float64(n))) - 1
	if index < 0 {
		index = 0
	}
	if index >= n {
		index = n - 1
	}
	return data[index]
}

func throughput(numRequests int, totalTime time.Duration) float64 {
	seconds := totalTime.Seconds()
	return float64(numRequests) / seconds
}
