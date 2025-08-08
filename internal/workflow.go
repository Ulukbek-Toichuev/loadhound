/*
LoadHound — Relentless load testing tool for SQL databases.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"context"
	"errors"
	"fmt"
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
		return fmt.Errorf("failed to get sql-client: %w", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			w.logger.Fatal().Err(err).Msg("failed to close sql-client")
		}
	}()

	cfgs := w.cfg.WorkflowConfig.Scenarios
	w.logger.Info().Int("scenarios_count", len(cfgs)).Msg("Initializing scenarios")
	scenarios, scMetrics, closers, err := initScenarios(ctx, w.logger, cfgs, client)
	if err != nil {
		return err
	}
	defer func() {
		for _, close := range closers {
			if err := close(); err != nil {
				w.logger.Fatal().Err(err).Msg("Failed to close prepared statement")
			}
		}
	}()

	g, ctx := errgroup.WithContext(ctx)
	startAt := time.Now()
	for _, sc := range scenarios {
		g.Go(func() error {
			return sc.Run(ctx)
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("one or more scenarios failed: %w", err)
	}

	w.logger.Info().Str("total_duration", time.Since(startAt).String()).Msg("All scenarios completed successfully")
	return GenerateReport(w.cfg, scMetrics)
}

func initScenarios(ctx context.Context, logger *zerolog.Logger, cfgs []*ScenarioConfig, client *SQLClient) ([]Scenario, []*Metric, []func() error, error) {
	closers := make([]func() error, 0)
	sharedId := NewSharedId()

	scenarios := make([]Scenario, 0)
	scenariosMetrics := make([]*Metric, 0)

	logger.Info().Int("scenarios_count", len(cfgs)).Msg("Initializing scenarios")
	for idx, cfg := range cfgs {
		// Init new logger for scenario from base logger
		scLogger := logger.With().Str("scenario_name", cfg.Name).Int("scenario_id", idx).Logger()

		// Get statement for each scenario
		statementExecutor, err := NewStatementExecutor(ctx, cfg.Pacing, cfg.StatementConfig, client)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create statement executor: %w", err)
		}
		closers = append(closers, statementExecutor.Close)

		// Get prepared threads list and thread metric object linked each thread
		pth, err := InitThreads(cfg.Threads, sharedId, statementExecutor, &scLogger)
		if err != nil {
			return nil, nil, nil, err
		}
		if pth == nil {
			return nil, nil, nil, errors.New("failed to init threads")
		}

		scLogger.Debug().Int("threads_initialized", len(pth)).Str("pacing", cfg.Pacing.String()).Msg("Threads initialized successfully")

		// Create metric for scenario
		m, err := NewMetric()
		if err != nil {
			return nil, nil, nil, err
		}
		// Create scenario
		var sc Scenario
		if cfg.Duration > 0 {
			sc = NewScenarioDur(&scLogger, cfg, pth, m)
		}
		if cfg.Iterations > 0 {
			sc = NewScenarioIter(&scLogger, cfg, pth, m)
		}

		scenarios = append(scenarios, sc)
		scenariosMetrics = append(scenariosMetrics, m)
	}
	return scenarios, scenariosMetrics, closers, nil
}

func (w *Workflow) getSQLClient(ctx context.Context) (*SQLClient, error) {
	client, err := NewSQLClient(ctx, w.cfg.DbConfig)
	if err != nil {
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

	// Apply minimum interval protection only
	if interval < rampUpMin {
		// Log warning that ramp-up will take longer than requested
		interval = rampUpMin
	}
	return interval
}

type StatementExecutor struct {
	Query      string
	Fn         ExecFunc
	Pacing     time.Duration
	stmtClient *PreparedStatement
}

func (stmtExec *StatementExecutor) Close() error {
	if stmtExec.stmtClient != nil {
		return stmtExec.stmtClient.Close()
	}
	return nil
}

func NewStatementExecutor(ctx context.Context, pacing time.Duration, cfg *StatementConfig, client *SQLClient) (*StatementExecutor, error) {
	execFunc, stmtClient, err := NewExecFunc(ctx, client, cfg.Query, cfg.Args)
	if err != nil {
		return nil, err
	}
	var stmtExec = &StatementExecutor{
		Query:      cfg.Query,
		Fn:         execFunc,
		Pacing:     pacing,
		stmtClient: stmtClient,
	}
	return stmtExec, nil
}

type ExecFunc func(ctx context.Context) *QueryResult

func NewExecFunc(ctx context.Context, client *SQLClient, query, args string) (ExecFunc, *PreparedStatement, error) {
	// If query is parametrizied
	// Create prepared statement
	// Get built-in functions
	if args != "" {
		generators, err := GetGenerators(args)
		if err != nil {
			return nil, nil, err
		}
		stmt, err := client.Prepare(ctx, query)
		if err != nil {
			return nil, nil, err
		}
		execFunc, err := getStmtFunc(stmt, query, generators)
		if err != nil {
			return nil, nil, err
		}
		return execFunc, stmt, nil
	}
	execFunc, err := getExecFunc(client, query)
	if err != nil {
		return nil, nil, err
	}
	return execFunc, nil, nil
}

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
