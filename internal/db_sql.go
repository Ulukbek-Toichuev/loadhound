/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"context"
	"database/sql"
	"fmt"
	"text/template"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type DriverType string

const (
	Postgres DriverType = "postgres"
	Mysql    DriverType = "mysql"
	Unknown  DriverType = "unknown"
)

type SQLWrapper struct {
	db         *sql.DB
	stmt       *sql.Stmt
	DriverType DriverType
}

func NewSQLWrapper(globalCtx context.Context, dbCfg *DbConfig, tmpl *template.Template) (*SQLWrapper, error) {
	// Get Database instance
	db, err := sql.Open(dbCfg.Driver, dbCfg.Dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to get db instance: %w", err)
	}

	// Set connection pool values
	if dbCfg.SQLConfig != nil {
		db.SetMaxOpenConns(dbCfg.SQLConfig.MaxOpenConnections)
		db.SetMaxIdleConns(dbCfg.SQLConfig.MaxIdleConnections)
		db.SetConnMaxLifetime(dbCfg.SQLConfig.ConnMaxLifeTime)
		db.SetConnMaxIdleTime(dbCfg.SQLConfig.ConnMaxIdleTime)
	}

	// Check connection to the database using ping()
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed ping db: %w", err)
	}

	// If prepared statement is enable
	// All queries or execs should be run using stmt instance
	// Instead of directly using db
	if dbCfg.SQLConfig != nil && dbCfg.SQLConfig.UseStmt {
		queryWithPlaceHolder, err := BuildQueryWithPlaceHolders(tmpl, dbCfg.Driver)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to get query with placeholders: %w", err)
		}
		stmt, err := db.Prepare(queryWithPlaceHolder)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to get prepared statement: %w", err)
		}
		return &SQLWrapper{db: db, stmt: stmt, DriverType: GetDriverType(dbCfg.Driver)}, nil
	}
	return &SQLWrapper{db: db, DriverType: GetDriverType(dbCfg.Driver)}, nil
}

func GetDriverType(driver string) DriverType {
	switch driver {
	case "postgres":
		return Postgres
	case "mysql":
		return Mysql
	default:
		return Unknown
	}
}

func (sw *SQLWrapper) IsStmt() bool {
	return sw.stmt != nil
}

func (sw *SQLWrapper) Close() error {
	if sw.stmt != nil {
		sw.stmt.Close()
	}
	return sw.db.Close()
}

func (sw *SQLWrapper) ExecWithLatency(globalCtx context.Context, query string) *QueryMetric {
	return measureLatency(query, func() (int64, error) {
		result, err := sw.db.ExecContext(globalCtx, query)
		if err != nil {
			return 0, err
		}
		return result.RowsAffected()
	})
}

func (sw *SQLWrapper) QueryRowsWithLatency(globalCtx context.Context, query string) *QueryMetric {
	return measureLatency(query, func() (int64, error) {
		rows, err := sw.db.QueryContext(globalCtx, query)
		if err != nil {
			return 0, err
		}
		return countRows(rows)
	})
}

func (sw *SQLWrapper) StmtExecWithLatency(globalCtx context.Context, args ...any) *QueryMetric {
	return measureLatency(fmt.Sprintf("%v", args...), func() (int64, error) {
		result, err := sw.stmt.ExecContext(globalCtx, args...)
		if err != nil {
			return 0, err
		}
		return result.RowsAffected()
	})
}

func (sw *SQLWrapper) StmtQueryRowsWithLatency(globalCtx context.Context, args ...any) *QueryMetric {
	return measureLatency(fmt.Sprintf("%v", args...), func() (int64, error) {
		rows, err := sw.stmt.QueryContext(globalCtx, args...)
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

func measureLatency(query string, f func() (int64, error)) *QueryMetric {
	start := time.Now()
	count, err := f()
	respTime := time.Since(start)

	return &QueryMetric{Query: query, ResponseTime: respTime, AffectedRows: count, Err: err}
}
