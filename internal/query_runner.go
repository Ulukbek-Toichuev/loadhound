/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"context"
	"fmt"
	"text/template"

	"github.com/Ulukbek-Toichuev/loadhound/internal/db"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
)

type QueryReader struct {
	conn *db.CustomConnPgx
	tmpl *template.Template
}

func NewQueryReader(conn *db.CustomConnPgx, tmpl *template.Template) *QueryReader {
	return &QueryReader{conn: conn, tmpl: tmpl}
}

func (q *QueryReader) Run(ctx context.Context) (*stat.QueryStat, error) {
	query, err := RenderTemplateQuery(q.tmpl)
	if err != nil {
		return nil, fmt.Errorf("template render failed: %v", err)
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

func (q *QueryExecutor) Run(ctx context.Context) (*stat.QueryStat, error) {
	query, err := RenderTemplateQuery(q.tmpl)
	if err != nil {
		return nil, fmt.Errorf("template render failed: %v", err)
	}

	return q.conn.ExecWithLatency(ctx, query), nil
}
