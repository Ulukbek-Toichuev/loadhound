/*
LoadHound — Relentless load testing tool for SQL databases.
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
		cfg:    cfg,
		logger: logger,
	}
}

// Run all scenarios in parallel and collect their metrics
func (w *Workflow) Run(ctx context.Context) error {
	// Get SQL-client instance
	client, err := w.getSQLClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	var (
		cfgs        = w.cfg.WorkflowConfig.Scenarios
		scenarios   = make([]Scenario, 0)
		threadStats = make([]*ThreadStat, 0)
		sharedId    = NewSharedId()

		threadCount int
	)
	w.logger.Info().Int("scenarios_count", len(cfgs)).Msg("Initializing scenarios")

	for idx, cfg := range cfgs {
		// Init new logger for scenario from base logger
		scLogger := w.logger.With().Str("scenario_name", cfg.Name).Int("scenario_id", idx).Logger()

		// Get ExecFunc
		var execFunc ExecFunc
		if len(cfg.StatementConfig.Args) != 0 {
			generators, err := GetGenerators(cfg.StatementConfig.Args)
			if err != nil {
				return err
			}
			stmt, err := client.Prepare(ctx, cfg.StatementConfig.Query)
			if err != nil {
				return err
			}
			defer stmt.Close()
			execFunc, err = getStmtFunc(stmt, cfg.StatementConfig.Query, generators)
			if err != nil {
				return err
			}
		} else {
			execFunc, err = getExecFunc(client, cfg.StatementConfig.Query)
			if err != nil {
				return err
			}
		}
		// Initialize threads for scenario
		statementExecutor := NewStatementExecutor(execFunc, cfg.Pacing, cfg.StatementConfig)
		pth, ths, err := InitThreads(cfg.Threads, sharedId, statementExecutor, &scLogger)
		if err != nil {
			return err
		}
		if pth == nil || threadStats == nil {
			return errors.New("failed to init threads")
		}

		threadStats = append(threadStats, ths...)
		threadCount += len(pth)
		w.logger.Debug().Int("threads_initialized", len(pth)).Str("pacing", cfg.Pacing.String()).Msg("Threads initialized successfully")

		// Create scenario
		if cfg.Duration > 0 {
			scenarios = append(scenarios, NewScenarioDur(&scLogger, cfg, pth))
		}
		if cfg.Iterations > 0 {
			scenarios = append(scenarios, NewScenarioIter(&scLogger, cfg, pth))
		}
	}

	// Init Global metrics collector
	globalMetric := NewGlobalMetric(threadStats)
	globalMetric.ThreadsTotal += threadCount

	globalMetric.StartAt = time.Now()
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
		return err
	}
	logger.Info().Msg("All scenarios completed successfully")
	globalMetric.Collect(metrics)
	return nil
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
