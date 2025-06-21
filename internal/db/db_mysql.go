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

	_ "github.com/go-sql-driver/mysql"
)

type MySQLWrapper struct {
	db *sql.DB
}

func NewMySQLWrapper(driverName, conn string) *MySQLWrapper {
	db, err := sql.Open(driverName, conn)
	if err != nil {
		pkg.PrintFatal("failed to parse pool config", err)
	}

	if err := db.Ping(); err != nil {
		pkg.PrintFatal("failed ping server", err)
	}
	return &MySQLWrapper{db: db}
}

func (p *MySQLWrapper) ExecWithLatency(ctx context.Context, query string) *stat.QueryStat {
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

func (p *MySQLWrapper) QueryRowsWithLatency(ctx context.Context, query string) *stat.QueryStat {
	return pkg.MeasureLatency(func() (int64, error) {
		rows, err := p.db.QueryContext(ctx, query)
		if err != nil {
			return 0, err
		}
		return pkg.CountRows(rows)
	})
}

func (p *MySQLWrapper) GetStmt(ctx context.Context, query string) (*sql.Stmt, error) {
	stm, err := p.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stm, nil
}

func (p *MySQLWrapper) StmtExecWithLatency(ctx context.Context, stmt *sql.Stmt, args ...any) *stat.QueryStat {
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

func (p *MySQLWrapper) StmtQueryRowsWithLatency(ctx context.Context, stmt *sql.Stmt, args ...any) *stat.QueryStat {
	return pkg.MeasureLatency(func() (int64, error) {
		rows, err := stmt.QueryContext(ctx, args...)
		if err != nil {
			return 0, err
		}
		return pkg.CountRows(rows)
	})
}
