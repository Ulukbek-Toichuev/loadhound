/*
LoadHound — Relentless load testing tool for SQL databases.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

const rampUpMin time.Duration = 10 * time.Millisecond

type Scenario interface {
	Run(ctx context.Context) error
}

// Workflow orchestrates the execution of multiple scenarios and collects their metrics
type Workflow struct {
	cfg    *RunConfig
	logger *zerolog.Logger
}

func NewWorkflow(cfg *RunConfig, logger *zerolog.Logger) *Workflow {
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
	for _, sc := range scenarios {
		g.Go(func() error {
			return sc.Run(ctx)
		})
	}
	if err := g.Wait(); err != nil {
		w.logger.Error().Err(err).Msg("One or more scenarios failed")
		return err
	}
	endAt := time.Now()
	globalMetric.EndAt = endAt

	w.logger.Info().Msg("All scenarios completed successfully")
	globalMetric.Collect()

	w.logger.Info().Str("total_duration", endAt.Sub(globalMetric.StartAt).String()).Msg("Test completed successfully")
	return GenerateReport(w.cfg, globalMetric)
}

func (w *Workflow) getSQLClient(ctx context.Context) (*SQLClient, error) {
	client, err := NewSQLClient(ctx, w.cfg.DbConfig)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to create SQL client")
		return nil, err
	}
	w.logger.Info().Str("driver", w.cfg.DbConfig.Driver).Str("dsn", w.cfg.DbConfig.Dsn).Msg("Database connection established")
	return client, nil
}

func calculateRampUpInterval(rampUp time.Duration, threads int) time.Duration {
	if threads <= 1 {
		return rampUp
	}
	// Calculate proper interval for full ramp-up span
	intervalNanos := rampUp / time.Duration(threads)
	interval := time.Duration(intervalNanos)

	// 50 000 000
	// 500 000
	// Apply minimum interval protection only
	if interval < rampUpMin {
		// Log warning that ramp-up will take longer than requested
		interval = rampUpMin
	}
	return interval
}

type StatementExecutor struct {
	fn     ExecFunc
	pacing time.Duration
	cfg    *StatementConfig
}

func NewStatementExecutor(fn ExecFunc, pacing time.Duration, cfg *StatementConfig) *StatementExecutor {
	return &StatementExecutor{
		fn:     fn,
		pacing: pacing,
		cfg:    cfg,
	}
}

type ExecFunc func(ctx context.Context) *QueryResult

// If SQL query is parametrizied, return statement function
func getStmtFunc(s *PreparedStatement, query string, generators []GeneratorFunc) (ExecFunc, error) {
	queryType := DetectQueryType(query)
	if queryType == "exec" {
		return func(ctx context.Context) *QueryResult {
			args := getArgs(generators)
			return s.StmtExecContext(ctx, query, args...)
		}, nil
	}
	if queryType == "query" {
		return func(ctx context.Context) *QueryResult {
			args := getArgs(generators)
			return s.StmtQueryContext(ctx, query, args...)
		}, nil
	}
	return nil, errors.New("unknown query type")
}

// If SQL query is text-based, return raw function
func getExecFunc(client *SQLClient, query string) (ExecFunc, error) {
	queryType := DetectQueryType(query)
	if queryType == "exec" {
		return func(ctx context.Context) *QueryResult {
			return client.ExecContext(ctx, query)
		}, nil
	}
	if queryType == "query" {
		return func(ctx context.Context) *QueryResult {
			return client.QueryContext(ctx, query)
		}, nil
	}
	return nil, errors.New("unknown query type")
}

// Get list of values for SQL query from functions
func getArgs(generators []GeneratorFunc) []any {
	args := make([]any, len(generators))
	for idx, fn := range generators {
		args[idx] = fn()
	}
	return args
}
