/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package executor

import (
	"context"
	"fmt"
	"text/template"

	"github.com/Ulukbek-Toichuev/loadhound/internal/db"
	"github.com/Ulukbek-Toichuev/loadhound/internal/parse"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
)

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

type Performer interface {
	Perform(ctx context.Context) (*stat.QueryStat, error)
}

type basePerformer struct {
	conn     *db.CustomConnPgx
	tmpl     *template.Template
	execFunc func(context.Context, *db.CustomConnPgx, string) *stat.QueryStat
}

func (p *basePerformer) Perform(ctx context.Context) (*stat.QueryStat, error) {
	query, err := parse.RenderTemplateQuery(p.tmpl)
	if err != nil {
		return nil, NewPerformerError("failed to render template", err)
	}
	return p.execFunc(ctx, p.conn, query), nil
}

func NewQueryReader(conn *db.CustomConnPgx, tmpl *template.Template) Performer {
	return &basePerformer{
		conn: conn,
		tmpl: tmpl,
		execFunc: func(ctx context.Context, conn *db.CustomConnPgx, query string) *stat.QueryStat {
			return conn.QueryRowsWithLatency(ctx, query)
		},
	}
}

func NewQueryExecutor(conn *db.CustomConnPgx, tmpl *template.Template) Performer {
	return &basePerformer{
		conn: conn,
		tmpl: tmpl,
		execFunc: func(ctx context.Context, conn *db.CustomConnPgx, query string) *stat.QueryStat {
			return conn.ExecWithLatency(ctx, query)
		},
	}
}

func NewPreparedStatementExecutor(conn *db.CustomConnPgx, tmpl *template.Template) Performer {
	return &basePerformer{}
}
