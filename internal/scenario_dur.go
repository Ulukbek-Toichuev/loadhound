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
)

type ScenarioDur struct {
	logger  *zerolog.Logger
	cfg     *ScenarioConfig
	threads []*Thread
	Metric  *Metric
}

func NewScenarioDur(logger *zerolog.Logger, cfg *ScenarioConfig, threads []*Thread, m *Metric) *ScenarioDur {
	return &ScenarioDur{
		logger:  logger,
		cfg:     cfg,
		threads: threads,
		Metric:  m,
	}
}

func (sc *ScenarioDur) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	timeOutCtx, cancel := context.WithTimeout(ctx, sc.cfg.Duration)
	defer cancel()

	// if user set ramp_up param to run threads gradually
	if sc.cfg.RampUp > 0 {
		// Calculation ramp_up interval, min value is 10 millisecond
		intervalDur := calculateRampUpInterval(sc.cfg.RampUp, sc.cfg.Threads)

		// Get ticker with ramp_up interval
		ticker := time.NewTicker(intervalDur)
		defer ticker.Stop()

		sc.Metric.SetStartTime(time.Now())
		sc.logger.Debug().Str("ramp_up_interval", intervalDur.String()).Int("total_threads", len(sc.threads)).Msg("Ramp-up configuration calculated")
		for _, thread := range sc.threads {
			select {
			case <-ctx.Done():
				sc.logger.Warn().Msg("Context cancelled during ramp-up")
				return ctx.Err()
			case <-ticker.C:
				wg.Add(1)
				go thread.RunOnDur(timeOutCtx, &wg)
			}
		}
	} else {
		sc.Metric.SetStartTime(time.Now())
		for _, thread := range sc.threads {
			select {
			case <-ctx.Done():
				sc.logger.Warn().Msg("Context cancelled during ramp-up")
				return ctx.Err()
			default:
				wg.Add(1)
				go thread.RunOnDur(timeOutCtx, &wg)
			}
		}
	}

	// wait until all threads finish their work
	wg.Wait()

	// sum metrics from threads
	sc.Metric.SetStopTime(time.Now())
	for _, th := range sc.threads {
		snapshot := th.Metric.GetSnapshot()
		sc.Metric.IterationsTotal += snapshot.IterationsTotal
		sc.Metric.RowsAffected += snapshot.RowsAffected
		sc.Metric.QueriesTotal += snapshot.QueriesTotal
		sc.Metric.ErrorsTotal += snapshot.ErrorsTotal
		if err := sc.Metric.Td.Merge(snapshot.Td); err != nil {
			return err
		}
		if len(snapshot.ErrMap) != 0 {
			for k, v := range snapshot.ErrMap {
				sc.Metric.ErrMap[k] = v
			}
		}
	}
	return nil
}
