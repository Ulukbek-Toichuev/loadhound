/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package db

import (
	"context"
	"database/sql"

	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"

	_ "github.com/lib/pq"
)

type PsqlWrapper struct {
	db *sql.DB
}

func NewPsqlWrapper(driverName, conn string) *PsqlWrapper {
	db, err := sql.Open(driverName, conn)
	if err != nil {
		pkg.PrintFatal("failed to parse pool config", err)
	}

	if err := db.Ping(); err != nil {
		pkg.PrintFatal("failed ping server", err)
	}
	return &PsqlWrapper{db: db}
}

func (p *PsqlWrapper) ExecWithLatency(ctx context.Context, query string) *stat.QueryStat {
	return pkg.MeasureLatency(func() (int64, error) {
		result, err := p.db.ExecContext(ctx, query)
		if err != nil {
			return 0, err
		}
		ra, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		return ra, nil
	})
}

func (p *PsqlWrapper) QueryRowsWithLatency(ctx context.Context, query string) *stat.QueryStat {
	return pkg.MeasureLatency(func() (int64, error) {
		rows, err := p.db.QueryContext(ctx, query)
		if err != nil {
			return 0, err
		}
		return pkg.CountRows(rows)
	})
}

func (p *PsqlWrapper) GetStmt(ctx context.Context, query string) (*sql.Stmt, error) {
	stm, err := p.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stm, nil
}

func (p *PsqlWrapper) StmtExecWithLatency(ctx context.Context, stmt *sql.Stmt, args ...any) *stat.QueryStat {
	return pkg.MeasureLatency(func() (int64, error) {
		result, err := stmt.ExecContext(ctx, args)
		if err != nil {
			return 0, err
		}
		ra, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		return ra, nil
	})
}

func (p *PsqlWrapper) StmtQueryRowsWithLatency(ctx context.Context, stmt *sql.Stmt, args ...any) *stat.QueryStat {
	return pkg.MeasureLatency(func() (int64, error) {
		rows, err := stmt.QueryContext(ctx, args...)
		if err != nil {
			return 0, err
		}
		return pkg.CountRows(rows)
	})
}
