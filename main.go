/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

const rampUpMin time.Duration = 10 * time.Millisecond

var (
	runTest = flag.String("run-test", "", "Path to *.toml file with test configuration")
	version = flag.Bool("version", false, "Get LoadHound current version")
)

func main() {
	globalCtx, globalStop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer globalStop()

	if err := Run(globalCtx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func getLogFilename() string {
	return fmt.Sprintf("loadhound_%s.log", time.Now().Format(time.RFC3339))
}

func GetLogger(cfg *internal.OutputConfig) (*zerolog.Logger, error) {
	if cfg == nil || cfg.LogConfig == nil || !cfg.LogConfig.ToConsole && !cfg.LogConfig.ToFile {
		discardLogger := zerolog.New(io.Discard)
		return &discardLogger, nil
	}

	writers := make([]io.Writer, 0)
	level, err := zerolog.ParseLevel(cfg.LogConfig.Level)
	if err != nil {
		return nil, err
	}
	zerolog.SetGlobalLevel(level)
	if cfg.LogConfig.ToConsole {
		writers = append(writers, &zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.TimeOnly})
	}

	var f *os.File
	if cfg.LogConfig.ToFile {
		var err error
		f, err = os.OpenFile(getLogFilename(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		writers = append(writers, f)
	}

	multiWriter := zerolog.MultiLevelWriter(writers...)
	logger := zerolog.New(multiWriter).With().Timestamp().Logger()
	return &logger, nil
}

func Run(globalCtx context.Context) error {
	flag.Usage = Usage
	flag.Parse()

	if len(os.Args) == 1 {
		flag.Usage()
		return nil
	}

	if *version {
		fmt.Printf("%s\n", pkg.Version)
		return nil
	}

	pkg.PrintBanner()

	var runTestConfig internal.RunTestConfig
	if err := internal.ReadConfigFile(*runTest, &runTestConfig); err != nil {
		return err
	}

	logger, err := GetLogger(runTestConfig.OutputConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	logger.Info().Msg("LoadHound started")
	logger.Debug().
		Str("config_file", *runTest).
		Int("scenarios_count", len(runTestConfig.WorkflowConfig.Scenarios)).
		Msg("Configuration loaded")

	client, err := internal.NewSQLClient(globalCtx, runTestConfig.DbConfig)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create SQL client")
		return err
	}

	logger.Info().
		Str("database_type", runTestConfig.DbConfig.Driver).
		Str("host", runTestConfig.DbConfig.Dsn).
		Msg("Database connection established")

	workflow := NewWorkflow(runTestConfig.WorkflowConfig.Scenarios)

	logger.Info().Msg("Starting load test execution")
	startTime := time.Now()

	err = workflow.RunTest(globalCtx, client, logger)

	duration := time.Since(startTime)
	if err != nil {
		logger.Error().
			Err(err).
			Str("total_duration", duration.String()).
			Msg("Load test failed")
		return err
	}

	logger.Info().
		Str("total_duration", duration.String()).
		Msg("Load test completed successfully")

	return nil
}

type Id struct {
	idx int
	mu  *sync.Mutex
}

func (i *Id) GetId() int {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.idx++
	return i.idx
}

type Workflow struct {
	scenarios []*internal.ScenarioConfig
}

func NewWorkflow(scenarios []*internal.ScenarioConfig) *Workflow {
	return &Workflow{
		scenarios: scenarios,
	}
}

func (w *Workflow) RunTest(ctx context.Context, sqlClient *internal.SQLClient, logger *zerolog.Logger) error {
	defer sqlClient.Close()
	if w.scenarios == nil {
		return errors.New("scenarios cannot be nil")
	}

	logger.Info().
		Int("scenarios_count", len(w.scenarios)).
		Msg("Initializing scenarios")

	globalId := Id{mu: &sync.Mutex{}}
	g, ctx := errgroup.WithContext(ctx)
	for i, sc := range w.scenarios {
		sc := sc
		scenarioIdx := i
		g.Go(func() error {
			scenarioLogger := logger.With().
				Int("scenario_id", scenarioIdx).
				Logger()

			return w.runScenario(ctx, sqlClient, sc, &globalId, &scenarioLogger)
		})
	}
	if err := g.Wait(); err != nil {
		logger.Error().Err(err).Msg("One or more scenarios failed")
		return err
	}

	logger.Info().Msg("All scenarios completed successfully")
	return nil
}

func (w *Workflow) runScenario(ctx context.Context, sqlClient *internal.SQLClient, sc *internal.ScenarioConfig, globalId *Id, logger *zerolog.Logger) error {
	logger.Info().
		Int("threads", sc.Threads).
		Str("duration", sc.Duration.String()).
		Int("iterations", sc.Iterations).
		Str("ramp_up", sc.RampUp.String()).
		Str("pacing", sc.Pacing.String()).
		Msg("Starting scenario execution")
	execFunc, err := GetExecFunc(ctx, sqlClient, sc.StatementConfig)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create execution function")
		return err
	}

	threads, err := initThreads(sc.Threads, globalId, execFunc, sc.Pacing, sc.StatementConfig.Query, logger)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to initialize threads")
		return err
	}

	var threadWg = &sync.WaitGroup{}
	if sc.RampUp > 0 {
		logger.Info().
			Str("ramp_up_duration", sc.RampUp.String()).
			Msg("Starting scenario with ramp-up")
		return w.runWithRampUp(ctx, sc, threads, threadWg, logger)
	}
	return w.runWithoutRampUp(ctx, sc, threads, threadWg)
}

func (w *Workflow) runWithRampUp(ctx context.Context, sc *internal.ScenarioConfig, threads []*Thread, threadWg *sync.WaitGroup, logger *zerolog.Logger) error {
	intervalDur := calculateRampUpInterval(sc.RampUp, sc.Threads)
	ticker := time.NewTicker(intervalDur)
	defer ticker.Stop()

	logger.Debug().
		Str("ramp_up_interval", intervalDur.String()).
		Int("total_threads", len(threads)).
		Msg("Ramp-up configuration calculated")

	if sc.Duration > 0 {
		timeOutCtx, cancel := context.WithTimeout(ctx, sc.Duration)
		defer cancel()
		for _, thread := range threads {
			select {
			case <-ctx.Done():
				logger.Warn().Msg("Context cancelled during ramp-up")
				return ctx.Err()
			case at := <-ticker.C:
				logger.Debug().
					Int("thread_id", thread.Id).
					Time("start_time", at).
					Msg("Starting thread")
				threadWg.Add(1)
				go thread.runOnDur(timeOutCtx, threadWg, at)
			}
		}
	} else if sc.Iterations > 0 {
		for _, thread := range threads {
			select {
			case <-ctx.Done():
				logger.Warn().Msg("Context cancelled during ramp-up")
				return ctx.Err()
			case at := <-ticker.C:
				logger.Debug().
					Int("thread_id", thread.Id).
					Int("iterations", sc.Iterations).
					Time("start_time", at).
					Msg("Starting thread with iterations")
				threadWg.Add(1)
				go thread.runOnIter(ctx, threadWg, at, sc.Iterations)
			}
		}
	}
	threadWg.Wait()
	return nil
}

func (w *Workflow) runWithoutRampUp(ctx context.Context, sc *internal.ScenarioConfig, threads []*Thread, threadWg *sync.WaitGroup) error {
	if sc.Duration > 0 {
		timeOutCtx, cancel := context.WithTimeout(ctx, sc.Duration)
		defer cancel()
		for _, thread := range threads {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				threadWg.Add(1)
				go thread.runOnDur(timeOutCtx, threadWg, time.Now())
			}
		}
	} else if sc.Iterations > 0 {
		for _, thread := range threads {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				threadWg.Add(1)
				go thread.runOnIter(ctx, threadWg, time.Now(), sc.Iterations)
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

func initThreads(threads int, globalId *Id, execFunc ExecFunc, pacing time.Duration, query string, logger *zerolog.Logger) ([]*Thread, error) {
	var preparedThreads = make([]*Thread, 0, threads)
	for i := 0; i < threads; i++ {
		m, err := internal.NewLocalMetric()
		if err != nil {
			return nil, err
		}
		preparedThreads = append(preparedThreads, NewThread(globalId.GetId(), m, execFunc, pacing, query, logger))
	}
	logger.Debug().
		Int("threads_initialized", threads).
		Str("pacing", pacing.String()).
		Msg("Threads initialized successfully")
	return preparedThreads, nil
}

type Thread struct {
	Id       int
	Metric   *internal.LocalMetric
	ExecFunc ExecFunc
	Pacing   time.Duration
	Query    string
	Logger   *zerolog.Logger
}

func NewThread(id int, metric *internal.LocalMetric, execFunc ExecFunc, pacing time.Duration, query string, logger *zerolog.Logger) *Thread {
	threadLogger := logger.With().
		Int("thread_id", id).
		Logger()
	return &Thread{
		Id:       id,
		Metric:   metric,
		ExecFunc: execFunc,
		Pacing:   pacing,
		Query:    query,
		Logger:   &threadLogger,
	}
}

func (t *Thread) runOnDur(ctx context.Context, wg *sync.WaitGroup, startAt time.Time) {
	defer wg.Done()
	t.Metric.StartAt(startAt)
	t.Logger.Debug().
		Time("start_time", startAt).
		Msg("Thread started (duration-based)")

	executionCount := 0
	for {
		select {
		case <-ctx.Done():
			t.Logger.Debug().
				Int("executions_completed", executionCount).
				Msg("Thread stopped due to context cancellation")
			return
		default:
		}
		t.exec(ctx)
		executionCount++

		if executionCount%100 == 0 {
			t.Logger.Debug().
				Int("executions_completed", executionCount).
				Msg("Thread execution progress")
		}
	}
}

func (t *Thread) runOnIter(ctx context.Context, wg *sync.WaitGroup, startAt time.Time, iterations int) {
	defer wg.Done()
	t.Metric.StartAt(startAt)
	t.Logger.Debug().
		Time("start_time", startAt).
		Int("total_iterations", iterations).
		Msg("Thread started (iteration-based)")

	for iter := 0; iter < iterations; iter++ {
		select {
		case <-ctx.Done():
			t.Logger.Debug().
				Int("completed_iterations", iter).
				Int("total_iterations", iterations).
				Msg("Thread stopped due to context cancellation")
			return
		default:
		}
		t.exec(ctx)

		if iterations >= 10 && (iter+1)%(iterations/10) == 0 {
			t.Logger.Debug().
				Int("completed_iterations", iter+1).
				Int("total_iterations", iterations).
				Float64("progress_percent", float64(iter+1)/float64(iterations)*100).
				Msg("Thread iteration progress")
		}
	}

	t.Logger.Debug().
		Int("completed_iterations", iterations).
		Msg("Thread completed all iterations")
}

func (t *Thread) exec(ctx context.Context) {
	start := time.Now()
	qm := t.ExecFunc(ctx, t.Query)
	t.Metric.Submit(qm)
	if qm.Err != nil {
		t.Logger.Error().
			Err(qm.Err).
			Str("duration", qm.ResponseTime.String()).
			Str("query", t.Query).
			Msg("Query execution failed")
	} else {
		t.Logger.Trace().
			Str("duration", qm.ResponseTime.String()).
			Msg("Query executed successfully")
	}

	evaluatePacing(start, t.Pacing)
	evaluatePacing(start, t.Pacing)
}

type ExecFunc func(ctx context.Context, query string) *internal.QueryResult

func GetExecFunc(ctx context.Context, client *internal.SQLClient, stmtCfg *internal.StatementConfig) (ExecFunc, error) {
	var execFunc ExecFunc
	if stmtCfg.Args != "" {
		s, err := client.Prepare(ctx, stmtCfg.Query)
		if err != nil {
			return nil, err
		}
		generators, err := internal.GetGenerators(stmtCfg.Args)
		if err != nil {
			return nil, err
		}
		queryType := internal.DetectQueryType(stmtCfg.Query)
		if queryType == "exec" {
			execFunc = func(ctx context.Context, query string) *internal.QueryResult {
				args := getArgs(generators)
				return s.StmtExecContext(ctx, query, args...)
			}
		}
		if queryType == "query" {
			execFunc = func(ctx context.Context, query string) *internal.QueryResult {
				args := getArgs(generators)
				return s.StmtQueryContext(ctx, query, args...)
			}
		}
	} else {
		queryType := internal.DetectQueryType(stmtCfg.Query)
		if queryType == "exec" {
			execFunc = client.ExecContext
		}
		if queryType == "query" {
			execFunc = client.QueryContext
		}
	}
	return execFunc, nil
}

func getArgs(generators []internal.GeneratorFunc) []any {
	args := make([]any, len(generators))
	for idx, fn := range generators {
		args[idx] = fn()
	}
	return args
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

func Usage() {
	usage := `Usage of LoadHound:
  -run-test string
      Path to your *.toml file for running test
  -version
      Get LoadHound version
`
	fmt.Println(usage)
}
