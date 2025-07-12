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

	// Read config from file
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

func (w *Workflow) RunTest(ctx context.Context, sqlClient *internal.SQLClient) error {
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
		scenariosWG.Add(1)
		go func(ctx context.Context) {
			defer scenariosWG.Done()

			execFunc, err := GetExecFunc(ctx, sqlClient, sc.StatementConfig)
			if err != nil {
				return
			}
			for i := 0; i < sc.Threads; i++ {
				m, err := internal.NewLocalMetric()
				if err != nil {
					return
				}
				threads = append(threads, NewThread(globalId.GetId(), m, execFunc, sc.Pacing, sc.StatementConfig.Query))
			}

			var isRampUpEnable bool
			if sc.RampUp > 0 {
				isRampUpEnable = true
			}

			var threadWG sync.WaitGroup
			if isRampUpEnable {
				intervalDur := time.Duration(int(sc.RampUp) / sc.Threads)
				threadTicker := time.NewTicker(intervalDur)
				if sc.Duration > 0 {
					timeOutCtx, cancel := context.WithTimeout(ctx, sc.Duration)
					defer cancel()
					for _, thread := range threads {
						select {
						case <-ctx.Done():
							return
						case at := <-threadTicker.C:
							threadWG.Add(1)
							fmt.Printf("thread-%d start at: %v\n", thread.Id, at.Format(time.TimeOnly))
							go thread.InitOnDur(timeOutCtx, &threadWG, at)
						}
					}
				} else if sc.Iterations > 0 {
					for _, thread := range threads {
						select {
						case <-ctx.Done():
							return
						case at := <-threadTicker.C:
							threadWG.Add(1)
							fmt.Printf("thread-%d start at: %v\n", thread.Id, at.Format(time.TimeOnly))
							go thread.InitOnIter(ctx, &threadWG, at, sc.Iterations)
						}
					}
				}
				threadTicker.Stop()
			} else {
				if sc.Duration > 0 {
					timeOutCtx, cancel := context.WithTimeout(ctx, sc.Duration)
					defer cancel()
					for _, thread := range threads {
						select {
						case <-ctx.Done():
							return
						default:
							threadWG.Add(1)
							go thread.InitOnDur(timeOutCtx, &threadWG, time.Now())
						}
					}
				} else if sc.Iterations > 0 {
					for _, thread := range threads {
						select {
						case <-ctx.Done():
							return
						default:
							threadWG.Add(1)
							go thread.InitOnIter(ctx, &threadWG, time.Now(), sc.Iterations)
						}
					}
				}
			}

			threadWG.Wait()
		}(ctx)
	}
	scenariosWG.Wait()
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
}

func NewThread(id int, metric *internal.LocalMetric, execFunc ExecFunc, pacing time.Duration, query string) *Thread {
	return &Thread{
		Id:       id,
		Metric:   metric,
		ExecFunc: execFunc,
		Pacing:   pacing,
		Query:    query,
	}
}

func (t *Thread) InitOnDur(ctx context.Context, wg *sync.WaitGroup, startAt time.Time) {
	defer wg.Done()
	t.Metric.StartAt(startAt)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			start := time.Now()
			qm, err := t.ExecFunc(ctx, t.Query)
			if err != nil {
				// TODO
			}
			fmt.Printf("thread-%d exec query: %v at: %v\n", t.Id, qm, time.Now().Format(time.TimeOnly))
			t.Metric.Submit(qm)
			evaluatePacing(start, t.Pacing)
		}
	}
}

func (t *Thread) InitOnIter(ctx context.Context, wg *sync.WaitGroup, startAt time.Time, iterations int) {
	defer wg.Done()
	t.Metric.StartAt(startAt)

	for iter := 0; iter < iterations; iter++ {
		select {
		case <-ctx.Done():
			return
		default:
			start := time.Now()
			qm, err := t.ExecFunc(ctx, t.Query)
			if err != nil {
				// TODO
			}
			fmt.Printf("thread-%d exec query: %v at: %v\n", t.Id, qm, time.Now().Format(time.TimeOnly))
			t.Metric.Submit(qm)
			evaluatePacing(start, t.Pacing)
		}
	}
}

type ExecFunc func(ctx context.Context, query string) (*internal.QueryResult, error)

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
			execFunc = func(ctx context.Context, query string) (*internal.QueryResult, error) {
				args := make([]any, 0, len(generators))
				for idx, fn := range generators {
					args[idx] = fn()
				}
				result, err := s.StmtExecContext(ctx, query, args...)
				if err != nil {
					return nil, err
				}
				return result, nil
			}
		} else if queryType == "query" {
			execFunc = func(ctx context.Context, query string) (*internal.QueryResult, error) {
				args := make([]any, 0, len(generators))
				for idx, fn := range generators {
					args[idx] = fn()
				}
				result, err := s.StmtQueryContext(ctx, query, args...)
				if err != nil {
					return nil, err
				}
				return result, nil
			}
		}
	} else {
		queryType := internal.DetectQueryType(stmtCfg.Query)
		if queryType == "exec" {
			execFunc = func(ctx context.Context, query string) (*internal.QueryResult, error) {
				result, err := client.ExecContext(ctx, query)
				if err != nil {
					return nil, err
				}
				return result, nil
			}
		} else if queryType == "query" {
			execFunc = func(ctx context.Context, query string) (*internal.QueryResult, error) {
				result, err := client.QueryContext(ctx, query)
				if err != nil {
					return nil, err
				}
				return result, nil
			}
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
