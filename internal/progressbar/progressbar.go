package progress

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
	"github.com/schollz/progressbar/v3"
)

type ProgressReporter interface {
	Start(ctx context.Context)
	Increment()
	Stop()
}

type noopProgressReporter struct{}

func (n *noopProgressReporter) Start(ctx context.Context) {}
func (n *noopProgressReporter) Increment()                {}
func (n *noopProgressReporter) Stop()                     {}

type BarConfig struct {
	MaxValue    int
	EnableBar   bool
	Description string             // например, "Running..."
	Width       int                // если 0, взять дефолт 15
	Theme       *progressbar.Theme // если nil, взять дефолт
	Logger      *zerolog.Logger    // для логирования ошибок
}

type ProgressBarWrapper struct {
	bar       *progressbar.ProgressBar
	incChan   chan struct{}
	doneChan  chan struct{}
	onceStart sync.Once
	logger    *zerolog.Logger
}

func NewProgressBarWrapper(cfg BarConfig) (ProgressReporter, error) {
	if !cfg.EnableBar {
		return &noopProgressReporter{}, nil
	}
	width := cfg.Width
	if width <= 0 {
		width = 15
	}
	desc := cfg.Description
	if desc == "" {
		desc = "Running..."
	}
	theme := cfg.Theme
	if theme == nil {
		theme = &progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}
	}
	bar := progressbar.NewOptions(cfg.MaxValue,
		progressbar.OptionSetPredictTime(false),
		progressbar.OptionShowCount(),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(width),
		progressbar.OptionSetDescription(desc),
		progressbar.OptionSetTheme(*theme),
	)
	bufSize := cfg.MaxValue
	if bufSize > 1000 || cfg.MaxValue <= 0 {
		bufSize = 1000
	}
	incChan := make(chan struct{}, bufSize)
	return &ProgressBarWrapper{
		bar:      bar,
		incChan:  incChan,
		doneChan: make(chan struct{}),
		logger:   cfg.Logger,
	}, nil
}

func (p *ProgressBarWrapper) Start(ctx context.Context) {
	p.onceStart.Do(func() {
		go func() {
			for {
				select {
				case <-ctx.Done():
					p.finalize()
					return
				case _, ok := <-p.incChan:
					if !ok {
						p.finalize()
						return
					}
					if err := p.bar.Add(1); err != nil {
						if p.logger != nil {
							p.logger.Error().Err(err).Msg("progress bar Add() failed")
						}
					}
				}
			}
		}()
	})
}

func (p *ProgressBarWrapper) Increment() {
	select {
	case p.incChan <- struct{}{}:
	default:
		if p.logger != nil {
			p.logger.Info().Msg("progress increment dropped: channel full")
		}
	}
}

func (p *ProgressBarWrapper) Stop() {
	close(p.incChan)
	<-p.doneChan
}

func (p *ProgressBarWrapper) finalize() {
	if err := p.bar.Finish(); err != nil {
		if p.logger != nil {
			p.logger.Error().Err(err).Msg("progress bar Finish failed")
		}
	}
	select {
	case <-p.doneChan:
	default:
		close(p.doneChan)
	}
}
