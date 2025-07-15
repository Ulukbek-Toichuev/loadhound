/*
LoadHound — Relentless load testing tool for SQL-oriented RDBMS.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

const rampUpMin time.Duration = 10 * time.Millisecond

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

type Workflow struct {
	scenarios []*ScenarioConfig
}

func NewWorkflow(scenarios []*ScenarioConfig) *Workflow {
	return &Workflow{
		scenarios: scenarios,
	}
}

func (w *Workflow) RunTest(ctx context.Context, sqlClient *SQLClient, logger *zerolog.Logger) (*GlobalMetric, error) {
	defer sqlClient.Close()

	var (
		threadsCount   int
		globalMetricWg = &sync.WaitGroup{}
		localMetricRec = make(chan *LocalMetric)
		globalId       = NewId()
	)

	// Initializing scenarios in parallel
	logger.Info().Int("scenarios_count", len(w.scenarios)).Msg("Initializing scenarios")
	g, ctx := errgroup.WithContext(ctx)
	for i, sc := range w.scenarios {
		sc := sc
		scenarioIdx := i
		threadsCount += sc.Threads
		g.Go(func() error {
			scenarioLogger := logger.With().Str("scenario_name", sc.Name).Int("scenario_id", scenarioIdx).Logger()
			return w.runScenario(ctx, sqlClient, sc, globalId, &scenarioLogger, localMetricRec)
		})
	}

	globalMetric := NewGlobalMetric()
	globalMetric.SetThreadCount(threadsCount)

	globalMetricWg.Add(1)
	metrics := make([]*LocalMetric, 0, threadsCount)
	go func(ctx context.Context) {
		defer globalMetricWg.Done()
		for range threadsCount {
			select {
			case <-ctx.Done():
				return
			case metric := <-localMetricRec:
				metrics = append(metrics, metric)
			}
		}
	}(ctx)

	if err := g.Wait(); err != nil {
		logger.Error().Err(err).Msg("One or more scenarios failed")
		return nil, err
	}
	logger.Info().Msg("All scenarios completed successfully")
	globalMetric.Collect(metrics)
	return globalMetric, nil
}

func (w *Workflow) runScenario(ctx context.Context, sqlClient *SQLClient, sc *ScenarioConfig, globalId *Id, logger *zerolog.Logger, localMetricRec chan *LocalMetric) error {
	logger.Info().Int("threads", sc.Threads).Str("duration", sc.Duration.String()).Int("iterations", sc.Iterations).Str("ramp_up", sc.RampUp.String()).Str("pacing", sc.Pacing.String()).Msg("Starting scenario execution")

	// Get query executor func that will be iteratively called
	execFunc, stmtClient, err := getExecFunc(ctx, sqlClient, sc.StatementConfig)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create execution function")
		return err
	}
	defer func() error {
		if stmtClient != nil {
			return stmtClient.Close()
		}
		return nil
	}()

	// Initializing threads
	threads, err := InitThreads(sc.Threads, globalId, execFunc, sc.Pacing, sc.StatementConfig.Query, logger, localMetricRec)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to initialize threads")
		return err
	}

	// Run with ramp_up if that param is set
	var threadWg = &sync.WaitGroup{}
	if sc.RampUp > 0 {
		logger.Info().Str("ramp_up_duration", sc.RampUp.String()).Msg("Starting scenario with ramp-up")
		return w.runWithRampUp(ctx, sc, threads, threadWg, logger)
	}
	return w.runWithoutRampUp(ctx, sc, threads, threadWg)
}

func (w *Workflow) runWithRampUp(ctx context.Context, sc *ScenarioConfig, threads []*Thread, threadWg *sync.WaitGroup, logger *zerolog.Logger) error {
	// Calculation ramp_up interval, min value is 10 millisecond
	intervalDur := calculateRampUpInterval(sc.RampUp, sc.Threads)

	// Get ticker with ramp_up interval
	ticker := time.NewTicker(intervalDur)
	defer ticker.Stop()

	logger.Debug().Str("ramp_up_interval", intervalDur.String()).Int("total_threads", len(threads)).Msg("Ramp-up configuration calculated")
	if sc.Duration > 0 { // Run threads based on duration
		timeOutCtx, cancel := context.WithTimeout(ctx, sc.Duration)
		defer cancel()
		for _, thread := range threads {
			select {
			case <-ctx.Done():
				logger.Warn().Msg("Context cancelled during ramp-up")
				return ctx.Err()
			case at := <-ticker.C:
				logger.Debug().Int("thread_id", thread.Id).Time("start_time", at).Msg("Starting thread")
				threadWg.Add(1)
				go thread.RunOnDur(timeOutCtx, threadWg, at)
			}
		}
	} else if sc.Iterations > 0 { // Run threads based on iterations
		for _, thread := range threads {
			select {
			case <-ctx.Done():
				logger.Warn().Msg("Context cancelled during ramp-up")
				return ctx.Err()
			case at := <-ticker.C:
				logger.Debug().Int("thread_id", thread.Id).Int("iterations", sc.Iterations).Time("start_time", at).Msg("Starting thread with iterations")
				threadWg.Add(1)
				go thread.RunOnIter(ctx, threadWg, at, sc.Iterations)
			}
		}
	}
	threadWg.Wait()
	return nil
}

func (w *Workflow) runWithoutRampUp(ctx context.Context, sc *ScenarioConfig, threads []*Thread, threadWg *sync.WaitGroup) error {
	if sc.Duration > 0 {
		timeOutCtx, cancel := context.WithTimeout(ctx, sc.Duration)
		defer cancel()
		for _, thread := range threads { // Run threads based on iterations
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				threadWg.Add(1)
				go thread.RunOnDur(timeOutCtx, threadWg, time.Now())
			}
		}
	} else if sc.Iterations > 0 {
		for _, thread := range threads { // Run threads based on iterations
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				threadWg.Add(1)
				go thread.RunOnIter(ctx, threadWg, time.Now(), sc.Iterations)
			}
		}
	}
	threadWg.Wait()
	return nil
}

func calculateRampUpInterval(rampUp time.Duration, threads int) time.Duration {
	if threads == 1 {
		return rampUp
	}

	intervalNanos := int64(rampUp) / int64(threads-1)
	interval := time.Duration(intervalNanos)

	if interval < rampUpMin {
		interval = rampUpMin
	}

	maxInterval := rampUp / time.Duration(threads)
	if interval > maxInterval {
		interval = maxInterval
	}
	return interval
}

type ExecFunc func(ctx context.Context, query string) *QueryResult

func getExecFunc(ctx context.Context, client *SQLClient, stmtCfg *StatementConfig) (ExecFunc, *PreparedStatement, error) {
	var execFunc ExecFunc
	if stmtCfg.Args != "" {
		s, err := client.Prepare(ctx, stmtCfg.Query)
		if err != nil {
			return nil, nil, err
		}
		generators, err := GetGenerators(stmtCfg.Args)
		if err != nil {
			return nil, nil, err
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
		return execFunc, s, nil
	}

	queryType := DetectQueryType(stmtCfg.Query)
	if queryType == "exec" {
		execFunc = client.ExecContext
	}
	if queryType == "query" {
		execFunc = client.QueryContext
	}
	return execFunc, nil, nil
}

func getArgs(generators []GeneratorFunc) []any {
	args := make([]any, len(generators))
	for idx, fn := range generators {
		args[idx] = fn()
	}
	return args
}
