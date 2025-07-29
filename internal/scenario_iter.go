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

type ScenarioIter struct {
	logger  *zerolog.Logger
	cfg     *ScenarioConfig
	threads []*Thread
}

func NewScenarioIter(logger *zerolog.Logger, cfg *ScenarioConfig, threads []*Thread) *ScenarioIter {
	return &ScenarioIter{
		logger:  logger,
		cfg:     cfg,
		threads: threads,
	}
}

func (sc *ScenarioIter) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	if sc.cfg.RampUp > 0 {
		// Calculation ramp_up interval, min value is 10 millisecond
		intervalDur := calculateRampUpInterval(sc.cfg.RampUp, sc.cfg.Threads)

		// Get ticker with ramp_up interval
		ticker := time.NewTicker(intervalDur)
		defer ticker.Stop()

		sc.logger.Debug().Str("ramp_up_interval", intervalDur.String()).Int("total_threads", len(sc.threads)).Msg("Ramp-up configuration calculated")
		for _, thread := range sc.threads {
			select {
			case <-ctx.Done():
				sc.logger.Warn().Msg("Context cancelled during ramp-up")
				return ctx.Err()
			case <-ticker.C:
				wg.Add(1)
				go thread.RunOnIter(ctx, &wg, sc.cfg.Iterations)
			}
		}
	} else {
		for _, thread := range sc.threads {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				wg.Add(1)
				go thread.RunOnIter(ctx, &wg, sc.cfg.Iterations)
			}
		}
	}
	wg.Wait()
	return nil
}
