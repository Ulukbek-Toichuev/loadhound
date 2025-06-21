package perform

import (
	"context"
	"database/sql"
	"text/template"

	"github.com/Ulukbek-Toichuev/loadhound/internal/db"
	"github.com/Ulukbek-Toichuev/loadhound/internal/parse"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
)

type PostgresPerform struct {
	conn     db.SQLWrapper
	stmt     *sql.Stmt
	tmpl     *template.Template
	execFunc func(context.Context, db.SQLWrapper, string) *stat.QueryStat
}

func (p *PostgresPerform) Perform(ctx context.Context) (*stat.QueryStat, error) {
	query, err := parse.RenderTemplateQuery(p.tmpl)
	if err != nil {
		return nil, NewPerformerError("failed to render template", err)
	}
	return p.execFunc(ctx, p.conn, query), nil
}

func (p *PostgresPerform) Close() error {
	if p.stmt != nil {
		return p.stmt.Close()
	}
	return nil
}

func NewQueryReader(conn db.SQLWrapper, tmpl *template.Template) Performer {
	return &PostgresPerform{
		conn: conn,
		tmpl: tmpl,
		execFunc: func(ctx context.Context, conn db.SQLWrapper, query string) *stat.QueryStat {
			return conn.QueryRowsWithLatency(ctx, query)
		},
	}
}

func NewQueryExecutor(conn db.SQLWrapper, tmpl *template.Template) Performer {
	return &PostgresPerform{
		conn: conn,
		tmpl: tmpl,
		execFunc: func(ctx context.Context, conn db.SQLWrapper, query string) *stat.QueryStat {
			return conn.ExecWithLatency(ctx, query)
		},
	}
}

func (p *PostgresPerform) GetStmt(ctx context.Context, query string) error {
	stmt, err := p.conn.GetStmt(ctx, query)
	if err != nil {
		return err
	}
	p.stmt = stmt
	return nil
}
