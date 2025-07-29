/*
LoadHound — Relentless load testing tool for SQL databases.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"sync"
	"time"

	"github.com/rs/zerolog"
)

func InitThreads(threads int, sharedId *SharedId, statementExecutor *StatementExecutor, logger *zerolog.Logger) ([]*Thread, []*ThreadStat, error) {
	var (
		threadStats     = make([]*ThreadStat, 0)
		preparedThreads = make([]*Thread, 0)
	)
	for i := 0; i < threads; i++ {
		ts, err := NewThreadStat()
		if err != nil {
			return nil, nil, err
		}
		threadStats = append(threadStats, ts)
		preparedThreads = append(preparedThreads, NewThread(sharedId.GetId(), ts, statementExecutor, logger))
	}
	return preparedThreads, threadStats, nil
}

type SharedId struct {
	idx int
	mu  *sync.Mutex
}

func NewSharedId() *SharedId {
	return &SharedId{mu: &sync.Mutex{}}
}

func (i *SharedId) GetId() int {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.idx++
	return i.idx
}

func EvaluatePacing(start time.Time, pacing time.Duration) {
	if pacing == 0 {
		return
	}
	elapsed := time.Since(start)
	if elapsed >= pacing {
		return
	}
	remaining := pacing - elapsed
	if remaining > 0 {
		time.Sleep(remaining)
	}
}
