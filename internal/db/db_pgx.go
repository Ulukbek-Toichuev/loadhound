/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package db

import (
	"context"
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"

	"github.com/jackc/pgx/v5"
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
		pkg.PrintFatal("failed to create connection pool", err)
	}
	err = pool.Ping(ctx)
	if err != nil {
		pkg.PrintFatal("failed ping server", err)
	}
	return &CustomConnPgx{pool}
}

func (c *CustomConnPgx) ExecWithLatency(ctx context.Context, query string) *stat.QueryStat {
	return measureLatency(func() (int64, error) {
		tag, err := c.Exec(ctx, query)
		if err != nil {
			return 0, err
		}
		return tag.RowsAffected(), nil
	})
}

func (c *CustomConnPgx) QueryRowsWithLatency(ctx context.Context, query string) *stat.QueryStat {
	return measureLatency(func() (int64, error) {
		rows, err := c.Query(ctx, query)
		if err != nil {
			return 0, err
		}

		defer rows.Close()
		var count int64
		if err := rows.Err(); err != nil {
			return count, err
		}
		for rows.Next() {
			count++
		}
		return count, nil
	})
}

func measureLatency(f func() (int64, error)) *stat.QueryStat {
	start := time.Now()
	count, err := f()
	latency := time.Since(start)

	return stat.NewQueryStat(latency, err, count)
}
