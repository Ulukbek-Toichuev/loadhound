package perform

import (
	"context"
	"text/template"

	"github.com/Ulukbek-Toichuev/loadhound/internal/db"
	"github.com/Ulukbek-Toichuev/loadhound/internal/parse"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
)

type SQLPerform struct {
	tmpl      *template.Template
	execFunc  func(ctx context.Context, query string) *stat.QueryStat
	stmtFunc  func(ctx context.Context, args ...any) *stat.QueryStat
	isStmt    bool
	closeFunc func() error
}

func (s *SQLPerform) Perform(ctx context.Context) (*stat.QueryStat, error) {
	if s.isStmt {
		args, err := parse.RenderQueryParams(s.tmpl)
		if err != nil {
			return nil, NewPerformerError("failed to render params", err)
		}
		return s.stmtFunc(ctx, args...), nil
	}

	query, err := parse.RenderTemplateQuery(s.tmpl)
	if err != nil {
		return nil, NewPerformerError("failed to render template", err)
	}
	return s.execFunc(ctx, query), nil
}

func (s *SQLPerform) Close() error {
	return s.closeFunc()
}

func newSQLPerform(
	tmpl *template.Template,
	isStmt bool,
	exec func(ctx context.Context, query string) *stat.QueryStat,
	stmt func(ctx context.Context, args ...any) *stat.QueryStat,
	closeFunc func() error,
) *SQLPerform {
	return &SQLPerform{
		tmpl:      tmpl,
		isStmt:    isStmt,
		execFunc:  exec,
		stmtFunc:  stmt,
		closeFunc: closeFunc,
	}
}

func NewSQLPerformExec(sqlExec db.SQLExecutor, tmpl *template.Template, isStmt bool) *SQLPerform {
	return newSQLPerform(
		tmpl,
		isStmt,
		func(ctx context.Context, query string) *stat.QueryStat {
			return sqlExec.ExecWithLatency(ctx, query)
		},
		func(ctx context.Context, args ...any) *stat.QueryStat {
			return sqlExec.StmtExecWithLatency(ctx, args...)
		},
		sqlExec.Close,
	)
}

func NewSQLPerformQueryRows(sqlExec db.SQLExecutor, tmpl *template.Template, isStmt bool) *SQLPerform {
	return newSQLPerform(
		tmpl,
		isStmt,
		func(ctx context.Context, query string) *stat.QueryStat {
			return sqlExec.QueryRowsWithLatency(ctx, query)
		},
		func(ctx context.Context, args ...any) *stat.QueryStat {
			return sqlExec.StmtQueryRowsWithLatency(ctx, args...)
		},
		sqlExec.Close,
	)
}
