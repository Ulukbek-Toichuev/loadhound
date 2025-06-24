/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package db

import (
	"context"
	"database/sql"
	"fmt"
	"text/template"

	"github.com/Ulukbek-Toichuev/loadhound/internal/model"
	"github.com/Ulukbek-Toichuev/loadhound/internal/parse"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type SQLExecutor interface {
	ExecWithLatency(ctx context.Context, query string) *stat.QueryStat
	QueryRowsWithLatency(ctx context.Context, query string) *stat.QueryStat
	StmtExecWithLatency(ctx context.Context, args ...any) *stat.QueryStat
	StmtQueryRowsWithLatency(ctx context.Context, args ...any) *stat.QueryStat
	Close() error
}

type SQLWrapper struct {
	db   *sql.DB
	stmt *sql.Stmt
}

func NewSQLWrapper(ctx context.Context, cfg *model.QuickRun, tmpl *template.Template) (*SQLWrapper, error) {
	db, err := sql.Open(cfg.Driver, cfg.Dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pool config: %w", err)
	}
	db.SetMaxOpenConns(2)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed ping server: %w", err)
	}
	if cfg.UseStmt {
		// query := `INSERT INTO tasks (title, description, priority)
		// VALUES (?, ?, ?);`
		queryWithPlaceHolder, err := parse.RenderQueryWithPlaceholders(tmpl, cfg.Driver)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to get query with placeholders: %w", err)
		}
		stmt, err := db.Prepare(queryWithPlaceHolder)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to get stmt: %w", err)
		}
		return &SQLWrapper{db: db, stmt: stmt}, nil
	}
	return &SQLWrapper{db: db}, nil
}

func (sw *SQLWrapper) Close() error {
	if sw.stmt != nil {
		sw.stmt.Close()
	}
	return sw.db.Close()
}

func (sw *SQLWrapper) ExecWithLatency(ctx context.Context, query string) *stat.QueryStat {
	return pkg.MeasureLatency(func() (int64, error) {
		result, err := sw.db.ExecContext(ctx, query)
		if err != nil {
			return 0, err
		}
		return result.RowsAffected()
	})
}

func (sw *SQLWrapper) QueryRowsWithLatency(ctx context.Context, query string) *stat.QueryStat {
	return pkg.MeasureLatency(func() (int64, error) {
		rows, err := sw.db.QueryContext(ctx, query)
		if err != nil {
			return 0, err
		}
		return countRows(rows)
	})
}

func (sw *SQLWrapper) StmtExecWithLatency(ctx context.Context, args ...any) *stat.QueryStat {
	return pkg.MeasureLatency(func() (int64, error) {
		result, err := sw.stmt.ExecContext(ctx, args...)
		if err != nil {
			return 0, err
		}
		return result.RowsAffected()
	})
}

func (sw *SQLWrapper) StmtQueryRowsWithLatency(ctx context.Context, args ...any) *stat.QueryStat {
	return pkg.MeasureLatency(func() (int64, error) {
		rows, err := sw.stmt.QueryContext(ctx, args...)
		if err != nil {
			return 0, err
		}
		return countRows(rows)
	})
}

func countRows(rows *sql.Rows) (int64, error) {
	defer rows.Close()
	var count int64
	if err := rows.Err(); err != nil {
		return count, err
	}
	for rows.Next() {
		count++
	}
	return count, nil
}
