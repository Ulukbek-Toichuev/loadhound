# LoadHound

![Go Version](https://img.shields.io/badge/Go-1.21+-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Status](https://img.shields.io/badge/status-stable-green)
![CI](https://github.com/Ulukbek-Toichuev/loadhound/actions/workflows/go.yml/badge.svg)
![coverage](https://raw.githubusercontent.com/Ulukbek-Toichuev/loadhound/badges/.badges/main/coverage.svg)

> A fast, lightweight CLI tool for load testing SQL databases with flexible configuration and built-in random data generators.

## Quick Start

```bash
# Download and install (replace with actual installation method)
go install github.com/Ulukbek-Toichuev/loadhound@latest

# Run a load test
loadhound -run my-test-scenario.toml
```

## Table of Contents

- [Description](#description)
- [Features](#features)
- [Example Scenario](#example-scenario)
- [Usage](#usage)
- [Supported Databases](#supported-databases)
- [License](#license)

## Description

**LoadHound** is a fast, lightweight CLI tool for load testing SQL-based databases.

## Features

- **Easy Configuration**: Write test scenarios in human-readable TOML format
- **Built-in Data Generators**: Generate realistic test data with built-in random functions
- **Multi-Database Support**: PostgreSQL and MySQL support out of the box
- **Flexible Load Patterns**: Configure duration, threads, pacing, and ramp-up strategies
- **Prepared Statements**: Optimized performance with parameterized queries
- **Connection Pooling**: Adjustable connection pool settings for optimal resource usage
- **Comprehensive Reporting**: Console and file output with detailed metrics
- **Configurable Logging**: Multiple log levels with file and console output options

## Installation

### Using Go Install

```bash
go install github.com/Ulukbek-Toichuev/loadhound@latest
```

### From Source

```bash
git clone https://github.com/Ulukbek-Toichuev/loadhound.git
cd loadhound
go build -o loadhound cmd/main.go
```

### Binary Releases

Download pre-compiled binaries from the [releases page](https://github.com/Ulukbek-Toichuev/loadhound/releases).

## Configuration Reference

### Database Configuration (`[db]`)

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `driver` | string | Yes | Database driver type | `"postgres"`, `"mysql"` |
| `dsn` | string | Yes | Database connection string | `"postgres://user:pass@host:port/db?sslmode=disable"` |

#### Connection Pool Configuration (`[db.conn_pool]`)

| Field | Type | Required | Description | Default | Example |
|-------|------|----------|-------------|---------|---------|
| `max_open_connections` | int | No | Maximum number of open connections to the database | Database driver default | `10` |
| `max_idle_connections` | int | No | Maximum number of idle connections in the pool | Database driver default | `5` |
| `conn_max_idle_time` | duration | No | Maximum time a connection can remain idle | Database driver default | `"30m"` |
| `conn_max_life_time` | duration | No | Maximum lifetime of a connection | Database driver default | `"1h"` |

### Workflow Configuration (`[workflow]`)

#### Scenarios (`[[workflow.scenarios]]`)

| Field | Type | Required | Description | Constraints | Example |
|-------|------|----------|-------------|-------------|---------|
| `name` | string | No | Human-readable scenario name | - | `"select_scenario"` |
| `iterations` | int | No* | Number of iterations per thread | Must be > 0 if duration not set | `100` |
| `duration` | duration | No* | Total runtime for the scenario | Mutually exclusive with iterations | `"30s"`, `"5m"` |
| `threads` | int | Yes | Number of concurrent threads | Must be >= 1 | `4` |
| `pacing` | duration | No | Delay between iterations within each thread | Cannot exceed duration | `"1s"`, `"500ms"` |
| `ramp_up` | duration | No | Time to gradually increase from 0 to N threads | - | `"10s"` |

*Either `iterations` or `duration` must be specified, but not both.

#### Statement Configuration (`[workflow.scenarios.statement]`)

| Field | Type | Required | Description | Constraints | Example |
|-------|------|----------|-------------|-------------|---------|
| `name` | string | No | Optional label for the statement | - | `"select_users"` |
| `query` | string | No* | SQL query to execute | Mutually exclusive with path_to_query | `"SELECT * FROM users WHERE id = $1"` |
| `path_to_query` | string | No* | Path to file containing the SQL query | Mutually exclusive with query | `"queries/select.sql"` |
| `args` | string | No | Parameters for prepared statements using built-in functions | - | `"randBool, randIntRange 1 100"` |

*Either `query` or `path_to_query` must be specified, but not both.

### Output Configuration (`[output]`)

#### Report Configuration (`[output.report]`)

| Field | Type | Required | Description | Default | Example |
|-------|------|----------|-------------|---------|---------|
| `to_file` | bool | No | Save report to JSON file | `false` | `true` |
| `to_console` | bool | No | Print report to console | `false` | `true` |

#### Log Configuration (`[output.log]`)

| Field | Type | Required | Description | Valid Values | Default | Example |
|-------|------|----------|-------------|--------------|---------|---------|
| `level` | string | No | Logging level | `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`, `"fatal"`, `"panic"` | `"info"` | `"debug"` |
| `to_file` | bool | No | Save logs to file | - | `false` | `true` |
| `to_console` | bool | No | Print logs to console | - | `false` | `true` |

### Duration Format

Duration values use Go's duration format:

- `"300ms"` - 300 milliseconds
- `"1.5s"` - 1.5 seconds  
- `"2m"` - 2 minutes
- `"1h30m"` - 1 hour 30 minutes

## Example Scenario

```toml
[db]
driver="postgres"
dsn="postgres://postgres:passwd@localhost:5432/loadhound_db?sslmode=disable"

# [db.conn_pool]
# max_open_connections=2
# max_idle_connections=2
# conn_max_idle_time="1m"
# conn_max_lifetime="1m"

[workflow]

[[workflow.scenarios]]
name="select_scenario"
duration="20s"
threads=4
pacing="1s"

[workflow.scenarios.statement]
name="select"
query="select * from loadhound_table lt where lt.rand_bool = $1 and lt.rand_int = $2;"
# path_to_query="query.sql"
args="randBool, randIntRange 100 1000"

[output.report]
to_file=true
to_console=true

[output.log]
to_file= true
to_console=true
level="info"
```

## Usage

### CLI Flags

| Flag | Description |
| -----|------------ |
| `-run` | Path to your `.toml` scenario file |
| `-version` | Print LoadHound version |

### Built-in parameter functions

| Function | Description | Return type |
| ---------|-------------|------------- |
| `randBool` | Returns `true` or `false` | `bool` |
| `randIntRange(a, b)` | Random integer in range | `int` |
| `randFloat64InRange(a, b)` | Random float in range | `float64` |
| `randUUID` | Random UUID string | `string` |
| `randStrRange(a, b)` | Random string of given length | `string` |
| `getTimestampNow` | Current timestamp | `int` |

### Logs

- Logs can be saved in file with name: `loadhound_2006-01-02T15:04:05Z07:00.log`
- LoadHound supports global log levels: `panic fatal error warn info debug trace`
- Logs format in `console`:

```bash
20:01:35 INF LoadHound started
20:01:35 INF Database connection established driver=postgres dsn=postgres://postgres:passwd@localhost:5432/loadhound_db?sslmode=disable
20:01:35 INF Initializing scenarios scenarios_count=1
20:01:50 ERR Query execution failed error=EOF duration=221.831166ms query="select * from loadhound_table lt where lt.rand_bool = $1 and lt.rand_int = $2;" scenario_id=0 scenario_name=select_scenario thread_id=1
20:01:50 ERR Query execution failed error=EOF duration=224.628666ms query="select * from loadhound_table lt where lt.rand_bool = $1 and lt.rand_int = $2;" scenario_id=0 scenario_name=select_scenario thread_id=4
20:01:55 INF All scenarios completed successfully
20:01:55 INF Test completed successfully total_duration=20.021146375s
```

- Logs format in `.log` file:

```log
{"level":"info","time":"2025-07-29T20:01:35+03:00","message":"LoadHound started"}
{"level":"info","driver":"postgres","dsn":"postgres://postgres:passwd@localhost:5432/loadhound_db?sslmode=disable","time":"2025-07-29T20:01:35+03:00","message":"Database connection established"}
{"level":"info","scenarios_count":1,"time":"2025-07-29T20:01:35+03:00","message":"Initializing scenarios"}
{"level":"error","scenario_name":"select_scenario","scenario_id":0,"thread_id":1,"error":"EOF","duration":"221.831166ms","query":"select * from loadhound_table lt where lt.rand_bool = $1 and lt.rand_int = $2;","time":"2025-07-29T20:01:50+03:00","message":"Query execution failed"}
{"level":"error","scenario_name":"select_scenario","scenario_id":0,"thread_id":4,"error":"EOF","duration":"224.628666ms","query":"select * from loadhound_table lt where lt.rand_bool = $1 and lt.rand_int = $2;","time":"2025-07-29T20:01:50+03:00","message":"Query execution failed"}
{"level":"info","time":"2025-07-29T20:01:55+03:00","message":"All scenarios completed successfully"}
{"level":"info","total_duration":"20.021146375s","time":"2025-07-29T20:01:55+03:00","message":"Test completed successfully"}
```

### Report

- Report can be saved in .json file with name: `loadhound_2006-01-02T15:04:05Z07:00.json`
- Report contains your .toml configuration and report
- Report format in `console`:

```bash
========== LoadHound Report ==========
duration: 20.021146375s

Query
total: 80 success_rate: 97.50% failed_rate: 2.50%
qps: 4.00 affected rows: 217
response time - min: 58.64975ms  max: 411.507041ms
response time - p50: 358.288667ms  p90: 391.202529ms  p95: 397.304208ms

Thread
thread count: 4
iteration count: 80

Errors
errors count: 2
1. EOF
```

- Report format in `.json` file:

```json
{
  "test_config": {
    "db": {
      "driver": "postgres",
      "dsn": "postgres://postgres:passwd@localhost:5432/loadhound_db?sslmode=disable",
      "conn_pool": null
    },
    "workflow": {
      "scenarios": [
        {
          "duration": "20s",
          "pacing": "1s",
          "ramp_up": "0s",
          "name": "select_scenario",
          "iterations": 0,
          "threads": 4,
          "statement": {
            "name": "select",
            "query": "select * from loadhound_table lt where lt.rand_bool = $1 and lt.rand_int = $2;",
            "args": "randBool, randIntRange 100 1000"
          }
        }
      ]
    },
    "output": {
      "report": {
        "to_file": true,
        "to_console": true
      },
      "log": {
        "level": "info",
        "to_file": true,
        "to_console": true
      }
    }
  },
  "test_duration": "20.021146375s",
  "query_data": {
    "queries_total": 80,
    "qps": "4.00",
    "min_resp_time": "58.64975ms",
    "max_resp_time": "411.507041ms",
    "success_rate": "97.50%",
    "failed_rate": "2.50%",
    "p50_resp_time": "358.288667ms",
    "p90_resp_time": "391.202529ms",
    "p95_resp_time": "397.304208ms",
    "affected_rows": 217,
    "err_total": 2,
    "top_errors": [
      "EOF"
    ]
  },
  "thread_data": {
    "thread_count": 4,
    "iteration_count": 80
  }
}
```
  
## Supported Databases

| Databases | Driver |
| ----------|----------- |
| MySQL | github.com/go-sql-driver/mysql |
| PostgreSQL | github.com/lib/pq |

## License

LoadHound is licensed under the [MIT License](LICENSE).
