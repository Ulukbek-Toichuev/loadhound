package internal

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

const rampUpMin time.Duration = 10 * time.Millisecond

type Workflow struct {
	scenarios []*ScenarioConfig
}

func NewWorkflow(scenarios []*ScenarioConfig) *Workflow {
	return &Workflow{
		scenarios: scenarios,
	}
}

func (w *Workflow) RunTest(ctx context.Context, sqlClient *SQLClient, logger *zerolog.Logger) error {
	defer sqlClient.Close()
	if w.scenarios == nil {
		return errors.New("scenarios cannot be nil")
	}
	logger.Info().Int("scenarios_count", len(w.scenarios)).Msg("Initializing scenarios")

	globalId := NewId()
	g, ctx := errgroup.WithContext(ctx)
	for i, sc := range w.scenarios {
		sc := sc
		scenarioIdx := i
		g.Go(func() error {
			scenarioLogger := logger.With().Str("scenario_name", sc.Name).Int("scenario_id", scenarioIdx).Logger()
			return w.runScenario(ctx, sqlClient, sc, globalId, &scenarioLogger)
		})
	}
	if err := g.Wait(); err != nil {
		logger.Error().Err(err).Msg("One or more scenarios failed")
		return err
	}
	logger.Info().Msg("All scenarios completed successfully")
	return nil
}

func (w *Workflow) runScenario(ctx context.Context, sqlClient *SQLClient, sc *ScenarioConfig, globalId *Id, logger *zerolog.Logger) error {
	logger.Info().Int("threads", sc.Threads).Str("duration", sc.Duration.String()).Int("iterations", sc.Iterations).Str("ramp_up", sc.RampUp.String()).Str("pacing", sc.Pacing.String()).Msg("Starting scenario execution")
	execFunc, err := GetExecFunc(ctx, sqlClient, sc.StatementConfig)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create execution function")
		return err
	}
	threads, err := InitThreads(sc.Threads, globalId, execFunc, sc.Pacing, sc.StatementConfig.Query, logger)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to initialize threads")
		return err
	}
	var threadWg = &sync.WaitGroup{}
	if sc.RampUp > 0 {
		logger.Info().Str("ramp_up_duration", sc.RampUp.String()).Msg("Starting scenario with ramp-up")
		return w.runWithRampUp(ctx, sc, threads, threadWg, logger)
	}
	return w.runWithoutRampUp(ctx, sc, threads, threadWg)
}

func (w *Workflow) runWithRampUp(ctx context.Context, sc *ScenarioConfig, threads []*Thread, threadWg *sync.WaitGroup, logger *zerolog.Logger) error {
	intervalDur := calculateRampUpInterval(sc.RampUp, sc.Threads)
	ticker := time.NewTicker(intervalDur)
	defer ticker.Stop()

	logger.Debug().Str("ramp_up_interval", intervalDur.String()).Int("total_threads", len(threads)).Msg("Ramp-up configuration calculated")
	if sc.Duration > 0 {
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
	} else if sc.Iterations > 0 {
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
		for _, thread := range threads {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				threadWg.Add(1)
				go thread.RunOnDur(timeOutCtx, threadWg, time.Now())
			}
		}
	} else if sc.Iterations > 0 {
		for _, thread := range threads {
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
