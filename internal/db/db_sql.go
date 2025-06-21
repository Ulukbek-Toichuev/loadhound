package db

import (
	"context"
	"database/sql"

	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
)

type SQLWrapper interface {
	ExecWithLatency(ctx context.Context, query string) *stat.QueryStat

	QueryRowsWithLatency(ctx context.Context, query string) *stat.QueryStat

	GetStmt(ctx context.Context, query string) (*sql.Stmt, error)

	StmtExecWithLatency(ctx context.Context, stmt *sql.Stmt, args ...any) *stat.QueryStat

	StmtQueryRowsWithLatency(ctx context.Context, stmt *sql.Stmt, args ...any) *stat.QueryStat
}
