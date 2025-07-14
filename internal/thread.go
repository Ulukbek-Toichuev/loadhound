package internal

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type ExecFunc func(ctx context.Context, query string) *QueryResult

func GetExecFunc(ctx context.Context, client *SQLClient, stmtCfg *StatementConfig) (ExecFunc, error) {
	var execFunc ExecFunc
	if stmtCfg.Args != "" {
		s, err := client.Prepare(ctx, stmtCfg.Query)
		if err != nil {
			return nil, err
		}
		generators, err := GetGenerators(stmtCfg.Args)
		if err != nil {
			return nil, err
		}
		queryType := DetectQueryType(stmtCfg.Query)
		if queryType == "exec" {
			execFunc = func(ctx context.Context, query string) *QueryResult {
				args := getArgs(generators)
				return s.StmtExecContext(ctx, query, args...)
			}
		}
		if queryType == "query" {
			execFunc = func(ctx context.Context, query string) *QueryResult {
				args := getArgs(generators)
				return s.StmtQueryContext(ctx, query, args...)
			}
		}
	} else {
		queryType := DetectQueryType(stmtCfg.Query)
		if queryType == "exec" {
			execFunc = client.ExecContext
		}
		if queryType == "query" {
			execFunc = client.QueryContext
		}
	}
	return execFunc, nil
}

func getArgs(generators []GeneratorFunc) []any {
	args := make([]any, len(generators))
	for idx, fn := range generators {
		args[idx] = fn()
	}
	return args
}

func InitThreads(threads int, globalId *Id, execFunc ExecFunc, pacing time.Duration, query string, logger *zerolog.Logger) ([]*Thread, error) {
	var preparedThreads = make([]*Thread, 0, threads)
	for i := 0; i < threads; i++ {
		m, err := NewLocalMetric()
		if err != nil {
			return nil, err
		}
		preparedThreads = append(preparedThreads, NewThread(globalId.GetId(), m, execFunc, pacing, query, logger))
	}
	logger.Debug().Int("threads_initialized", threads).Str("pacing", pacing.String()).Msg("Threads initialized successfully")
	return preparedThreads, nil
}

type Thread struct {
	Id       int
	Metric   *LocalMetric
	ExecFunc ExecFunc
	Pacing   time.Duration
	Query    string
	Logger   *zerolog.Logger
}

func NewThread(id int, metric *LocalMetric, execFunc ExecFunc, pacing time.Duration, query string, logger *zerolog.Logger) *Thread {
	threadLogger := logger.With().Int("thread_id", id).Logger()
	return &Thread{
		Id:       id,
		Metric:   metric,
		ExecFunc: execFunc,
		Pacing:   pacing,
		Query:    query,
		Logger:   &threadLogger,
	}
}

func (t *Thread) RunOnDur(ctx context.Context, wg *sync.WaitGroup, startAt time.Time) {
	defer wg.Done()
	t.Metric.StartAt(startAt)
	t.Logger.Debug().Time("start_time", startAt).Msg("Thread started (duration-based)")

	executionCount := 0
	for {
		select {
		case <-ctx.Done():
			t.Logger.Debug().Int("executions_completed", executionCount).Msg("Thread stopped due to context cancellation")
			return
		default:
		}
		t.exec(ctx)
		executionCount++

		if executionCount%100 == 0 {
			t.Logger.Debug().Int("executions_completed", executionCount).Msg("Thread execution progress")
		}
	}
}

func (t *Thread) RunOnIter(ctx context.Context, wg *sync.WaitGroup, startAt time.Time, iterations int) {
	defer wg.Done()
	t.Metric.StartAt(startAt)
	t.Logger.Debug().Time("start_time", startAt).Int("total_iterations", iterations).Msg("Thread started (iteration-based)")

	for iter := 0; iter < iterations; iter++ {
		select {
		case <-ctx.Done():
			t.Logger.Debug().Int("completed_iterations", iter).Int("total_iterations", iterations).Msg("Thread stopped due to context cancellation")
			return
		default:
		}
		t.exec(ctx)
		t.Metric.AddIters()

		if iterations >= 10 && (iter+1)%(iterations/10) == 0 {
			t.Logger.Debug().Int("completed_iterations", iter+1).Int("total_iterations", iterations).Float64("progress_percent", float64(iter+1)/float64(iterations)*100).Msg("Thread iteration progress")
		}
	}

	t.Logger.Debug().Int("completed_iterations", iterations).Msg("Thread completed all iterations")
}

func (t *Thread) exec(ctx context.Context) {
	start := time.Now()
	qm := t.ExecFunc(ctx, t.Query)
	t.Metric.Submit(qm)
	if qm.Err != nil {
		t.Logger.Error().Err(qm.Err).Str("duration", qm.ResponseTime.String()).Str("query", t.Query).Msg("Query execution failed")
	} else {
		t.Logger.Trace().Str("duration", qm.ResponseTime.String()).Msg("Query executed successfully")
	}

	evaluatePacing(start, t.Pacing)
}

func evaluatePacing(start time.Time, pacing time.Duration) {
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

type Id struct {
	idx int
	mu  *sync.Mutex
}

func NewId() *Id {
	return &Id{mu: &sync.Mutex{}}
}

func (i *Id) GetId() int {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.idx++
	return i.idx
}
