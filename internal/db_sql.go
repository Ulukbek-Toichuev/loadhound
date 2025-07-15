/*
LoadHound — Simple load testing cli tool for SQL-oriented RDBMS.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type SQLClient struct {
	DB *sql.DB
}

func NewSQLClient(ctx context.Context, dbCfg *DbConfig) (*SQLClient, error) {
	db, err := sql.Open(dbCfg.Driver, dbCfg.Dsn)
	if err != nil {
		return nil, err
	}
	setConnPoolParams(dbCfg.ConnPoolCfg, db)
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}
	return &SQLClient{DB: db}, nil
}

func (sa *SQLClient) Close() error {
	return sa.DB.Close()
}

type PreparedStatement struct {
	stmt   *sql.Stmt
	client *SQLClient
}

func (sa *SQLClient) Prepare(ctx context.Context, query string) (*PreparedStatement, error) {
	stmt, err := sa.DB.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &PreparedStatement{
		stmt:   stmt,
		client: sa,
	}, nil
}

func (ps *PreparedStatement) Close() error {
	return ps.stmt.Close()
}

func setConnPoolParams(cfg *ConnPoolCfg, db *sql.DB) {
	if cfg == nil {
		return
	}
	if cfg.MaxOpenConnections > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConnections)
	}
	if cfg.MaxIdleConnections > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConnections)
	}
	if cfg.ConnMaxLifeTime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifeTime)
	}
	if cfg.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}
}

type QueryResult struct {
	Args         []any
	Query        string
	RowsAffected int64
	ResponseTime time.Duration
	Err          error
}

func (sa *SQLClient) ExecContext(ctx context.Context, query string) *QueryResult {
	startTime := time.Now()
	result, err := sa.DB.ExecContext(ctx, query)

	queryResult := &QueryResult{Query: query, ResponseTime: time.Since(startTime)}
	if err != nil {
		queryResult.Err = err
		return queryResult
	}
	r, err := result.RowsAffected()
	if err != nil {
		queryResult.Err = err
		return queryResult
	}
	queryResult.RowsAffected = r
	return queryResult
}

func (sa *SQLClient) QueryContext(ctx context.Context, query string) *QueryResult {
	startTime := time.Now()
	rows, err := sa.DB.QueryContext(ctx, query)

	queryResult := &QueryResult{Query: query, ResponseTime: time.Since(startTime)}
	if err != nil {
		queryResult.Err = err
		return queryResult
	}
	r, err := countRows(rows)
	if err != nil {
		queryResult.Err = err
		return queryResult
	}
	queryResult.RowsAffected = r
	return queryResult
}

func (sa *PreparedStatement) StmtExecContext(ctx context.Context, query string, args ...any) *QueryResult {
	startTime := time.Now()
	result, err := sa.stmt.ExecContext(ctx, args...)

	queryResult := &QueryResult{Query: query, Args: args, ResponseTime: time.Since(startTime)}
	if err != nil {
		queryResult.Err = err
		return queryResult
	}
	r, err := result.RowsAffected()
	if err != nil {
		queryResult.Err = err
		return queryResult
	}
	queryResult.RowsAffected = r
	return queryResult
}

func (sa *PreparedStatement) StmtQueryContext(ctx context.Context, query string, args ...any) *QueryResult {
	startTime := time.Now()
	rows, err := sa.stmt.QueryContext(ctx, args...)

	queryResult := &QueryResult{Query: query, Args: args, ResponseTime: time.Since(startTime)}
	if err != nil {
		queryResult.Err = err
		return queryResult
	}
	r, err := countRows(rows)
	if err != nil {
		queryResult.Err = err
		return queryResult
	}
	queryResult.RowsAffected = r
	return queryResult
}

func countRows(rows *sql.Rows) (int64, error) {
	if rows == nil {
		return 0, errors.New("rows is nil")
	}
	defer rows.Close()
	var count int64
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		return count, err
	}
	return count, nil
}

func DetectQueryType(query string) string {
	trimmed := strings.TrimSpace(query)
	lower := strings.ToLower(trimmed)
	fields := strings.Fields(lower)

	if len(fields) == 0 {
		return "unknown"
	}

	switch fields[0] {
	case "select", "show", "describe", "explain":
		return "query"
	case "insert", "update", "delete", "create", "drop", "alter", "truncate":
		return "exec"
	case "with":
		if strings.Contains(lower, "select") {
			return "query"
		}
		return "exec"
	case "call", "do":
		return "exec"
	default:
		return "exec"
	}
}
