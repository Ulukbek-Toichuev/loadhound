package internal

import (
	"context"
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/pkg"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CustomConnPgx struct {
	*pgxpool.Pool
}

func GetPgxConn(ctx context.Context, url string) *CustomConnPgx {
	poolConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		pkg.PrintFatal("failed to parse pool config", err)
	}
	poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		pkg.PrintFatal("failed create connection pool", err)
	}
	err = pool.Ping(ctx)
	if err != nil {
		pkg.PrintFatal("failed ping server", err)
	}
	return &CustomConnPgx{pool}
}

func (c *CustomConnPgx) ExecWithLatency(ctx context.Context, query string) (pgconn.CommandTag, time.Duration, error) {
	start := time.Now()
	tag, err := c.Exec(ctx, query)
	latency := time.Since(start)

	if err != nil {
		return pgconn.CommandTag{}, latency, err
	}
	return tag, latency, nil
}

func (c *CustomConnPgx) QueryRowsWithLatency(ctx context.Context, query string) (int64, time.Duration, error) {
	var r int64
	start := time.Now()
	rows, err := c.Query(ctx, query)
	latency := time.Since(start)

	if err != nil {
		return r, latency, err
	}

	defer rows.Close()
	for rows.Next() {
		r++
	}

	return r, latency, nil
}

func (c *CustomConnPgx) QueryWithLatency(ctx context.Context, query string) (pgconn.CommandTag, time.Duration, error) {
	start := time.Now()
	rows, err := c.Query(ctx, query)
	latency := time.Since(start)

	if err != nil {
		return pgconn.CommandTag{}, latency, err
	}

	return rows.CommandTag(), latency, nil
}
