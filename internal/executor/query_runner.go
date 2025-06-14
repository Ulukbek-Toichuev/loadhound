/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package executor

import (
	"context"
	"text/template"

	"github.com/Ulukbek-Toichuev/loadhound/internal/db"
	"github.com/Ulukbek-Toichuev/loadhound/internal/parse"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
)

type Performer interface {
	Perform(ctx context.Context) (*stat.QueryStat, error)
}

type PerformerError struct {
	Message string
	Err     error
}

func NewPerformerError(msg string, err error) *PerformerError {
	return &PerformerError{msg, err}
}

func (e *PerformerError) Error() string {
	return e.Message
}

func (e *PerformerError) Unwrap() error {
	return e.Err
}

type QueryReader struct {
	conn *db.CustomConnPgx
	tmpl *template.Template
}

func NewQueryReader(conn *db.CustomConnPgx, tmpl *template.Template) *QueryReader {
	return &QueryReader{conn: conn, tmpl: tmpl}
}

func (q *QueryReader) Perform(ctx context.Context) (*stat.QueryStat, error) {
	query, err := parse.RenderTemplateQuery(q.tmpl)
	if err != nil {
		return nil, NewPerformerError("failed render template", err)
	}

	return q.conn.QueryRowsWithLatency(ctx, query), nil
}

type QueryExecutor struct {
	conn *db.CustomConnPgx
	tmpl *template.Template
}

func NewQueryExecutor(conn *db.CustomConnPgx, tmpl *template.Template) *QueryExecutor {
	return &QueryExecutor{conn: conn, tmpl: tmpl}
}

func (q *QueryExecutor) Perform(ctx context.Context) (*stat.QueryStat, error) {
	query, err := parse.RenderTemplateQuery(q.tmpl)
	if err != nil {
		return nil, NewPerformerError("failed render template", err)
	}

	return q.conn.ExecWithLatency(ctx, query), nil
}
