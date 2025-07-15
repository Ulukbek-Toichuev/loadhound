/*
LoadHound — Relentless load testing tool for SQL-oriented RDBMS.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

func GetLogger(cfg *OutputConfig) (*zerolog.Logger, error) {
	if cfg == nil || cfg.LogConfig == nil || (!cfg.LogConfig.ToConsole && !cfg.LogConfig.ToFile) {
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
		var (
			err      error
			filename string = fmt.Sprintf("loadhound_%s.log", time.Now().Format(time.RFC3339))
		)
		f, err = os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		writers = append(writers, f)
	}

	multiWriter := zerolog.MultiLevelWriter(writers...)
	logger := zerolog.New(multiWriter).With().Timestamp().Logger()
	return &logger, nil
}
