/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package perform

import (
	"context"
	"fmt"

	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
)

type Performer interface {
	Perform(ctx context.Context) (*stat.QueryStat, error)
	Close() error
}

type PerformerError struct {
	Message string
	Err     error
}

func NewPerformerError(msg string, err error) *PerformerError {
	return &PerformerError{msg, err}
}

func (e *PerformerError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *PerformerError) Unwrap() error {
	return e.Err
}
