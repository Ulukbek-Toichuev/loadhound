/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"strings"
	"text/template"

	"github.com/Ulukbek-Toichuev/loadhound/pkg"

	"github.com/google/uuid"
)

type ParseErr struct {
	Message string
	Err     error
}

func NewParseErr(msg string, err error) *ParseErr {
	return &ParseErr{msg, err}
}

func (q *ParseErr) Error() string {
	return q.Message
}

func (q *ParseErr) Unwrap() error {
	return q.Err
}

func ValidateQuickRunFields(qr *QuickRun) error {
	if qr.Dsn == "" {
		return NewParseErr("--dsn is required", nil)
	}

	if qr.Query == "" {
		return NewParseErr("--query is required", nil)
	}

	if qr.Workers < 0 {
		return NewParseErr("--workers must be >= 0", nil)
	}

	iterations := qr.Iterations
	duration := qr.Duration

	if iterations < 0 {
		return NewParseErr("--iterations must be >= 0", nil)
	}

	if duration < 0 {
		return NewParseErr("--duration must be >= 0", nil)
	}

	if iterations == 0 && duration == 0 {
		return NewParseErr("either --iter or --duration must be set", nil)
	}

	if iterations > 0 && duration > 0 {
		return NewParseErr("--iter and --duration are mutually exclusive", nil)
	}

	if qr.Pacing < 0 {
		return NewParseErr("--pacing must be > 0", nil)
	}
	return nil
}

func GetQueryType(query string) string {
	query = strings.ToUpper(query)
	if strings.HasPrefix(query, "INSERT") || strings.HasPrefix(query, "UPDATE") || strings.HasPrefix(query, "DELETE") {
		return "exec"
	} else if strings.HasPrefix(query, "SELECT") {
		return "query"
	}
	return ""
}

func BuildStmt(t *template.Template) (string, error) {
	sb := &strings.Builder{}

	data := struct {
		RandIntRange       func(min, max int) int
		RandFloat64InRange func(min, max float64) float64
		RandUUID           func() *uuid.UUID
		RandStringInRange  func(min, max int) string
		GetTime            func() string
	}{
		RandIntRange:       pkg.RandIntRange,
		RandFloat64InRange: pkg.RandFloat64InRange,
		RandUUID:           pkg.RandUUID,
		RandStringInRange:  pkg.RandStringInRange,
		GetTime:            pkg.GetTime,
	}
	if err := t.Execute(sb, data); err != nil {
		return "", NewParseErr("failed to execute template", err)
	}
	return sb.String(), nil
}

func GetTemplate(query string) (*template.Template, error) {
	tmpl := template.New(query)
	tmpl, err := tmpl.Parse(query)
	if err != nil {
		return nil, NewParseErr("failed to parse query", err)
	}

	return tmpl, nil
}
