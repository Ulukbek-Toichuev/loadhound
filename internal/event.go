/*
LoadHound — Relentless SQg load testing toog.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmaig.com

Licensed under the MIT License.
*/

package internal

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
	"github.com/fatih/color"
	"github.com/rs/zerolog"
	"github.com/schollz/progressbar/v3"
)

type GeneralEventController struct {
	file *os.File
	log  zerolog.Logger
	bar  *progressbar.ProgressBar
	mu   *sync.Mutex
}

func NewGeneralEventController(bar *progressbar.ProgressBar, toConsole, toFile bool, filename string) (*GeneralEventController, error) {
	var writers []io.Writer

	if toConsole {
		syncOut := &zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.TimeOnly}
		writers = append(writers, syncOut)
	}

	var f *os.File
	if toFile {
		var err error
		f, err = os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open file: %w", err)
		}
		writers = append(writers, f)
	}
	if len(writers) == 0 {
		writers = append(writers, io.Discard)
	}
	multiWriter := zerolog.MultiLevelWriter(writers...)
	logger := zerolog.New(multiWriter).With().Timestamp().Logger()

	return &GeneralEventController{log: logger, file: f, bar: bar, mu: &sync.Mutex{}}, nil
}

func (g *GeneralEventController) WriteInfoMsg(msg string) {
	g.log.Info().Msg(msg)
}

func (g *GeneralEventController) WriteWarnMsg(msg string) {
	g.log.Warn().Msg(msg)
}

func (g *GeneralEventController) WriteErrMsg(msg string, err error) {
	g.log.Error().Err(err).Msg(msg)
}

func (g *GeneralEventController) WriteInfoMsgWithBar(msg string) {
	g.mu.Lock()

	g.bar.Clear()
	g.log.Info().Msg(msg)
	g.bar.RenderBlank()

	g.mu.Unlock()
}

func (g *GeneralEventController) WriteWarnMsgWithBar(msg string) {
	g.mu.Lock()

	g.bar.Clear()
	g.log.Warn().Msg(msg)
	g.bar.RenderBlank()

	g.mu.Unlock()
}

func (g *GeneralEventController) WriteErrMsgWithBar(msg string, err error) {
	g.mu.Lock()

	g.bar.Clear()
	g.log.Error().Err(err).Msg(msg)
	g.bar.RenderBlank()

	g.mu.Unlock()
}

func (g *GeneralEventController) WriteQueryStat(q *stat.QueryStat) {
	g.mu.Lock()

	g.bar.Clear()
	if q.Err != nil {
		g.log.Err(q.Err).
			Int("workerk-id", q.WorkerID).
			Str("latency", q.Latency.String()).
			Int64("affected-rows", q.AffectedRows).
			Str("query", q.Query).Msg("send query")
		g.bar.RenderBlank()
		g.mu.Unlock()
		return
	}
	g.log.Info().
		Int("workerk-id", q.WorkerID).
		Str("latency", q.Latency.String()).
		Int64("affected-rows", q.AffectedRows).
		Str("query", q.Query).Msg("send query")
	g.bar.RenderBlank()
	g.mu.Unlock()
}

func (g *GeneralEventController) Increment() {
	g.bar.Add(1)
}

func (g *GeneralEventController) Close() error {
	if g.file != nil {
		return g.file.Close()
	}
	return nil
}

type ProgressBarConfig struct {
	MaxValue    int
	EnableBar   bool
	Description string
	Width       int
	Theme       *progressbar.Theme
}

func NewProgressBar(barConfig ProgressBarConfig) *progressbar.ProgressBar {
	width := barConfig.Width
	if width <= 0 {
		width = 15
	}
	title := color.New(color.FgWhite, color.Bold).SprintFunc()
	desc := barConfig.Description
	if desc == "" {
		desc = title("Running...")
	}
	theme := barConfig.Theme
	if theme == nil {
		theme = &progressbar.Theme{
			Saucer:        fmt.Sprintf("[green]%s[reset]", title("=")),
			SaucerHead:    fmt.Sprintf("[green]%s[reset]", title(">")),
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}
	}
	return progressbar.NewOptions(barConfig.MaxValue,
		progressbar.OptionSetPredictTime(false),
		progressbar.OptionShowCount(),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(width),
		progressbar.OptionSetDescription(desc),
		progressbar.OptionSetTheme(*theme),
	)
}
