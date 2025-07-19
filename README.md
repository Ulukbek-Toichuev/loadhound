# LoadHound

![Go Version](https://img.shields.io/badge/Go-1.21+-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Status](https://img.shields.io/badge/status-alpha-orange)
![GitHub all releases](https://img.shields.io/github/downloads/Ulukbek-Toichuev/loadhound/total)
![CI](https://github.com/Ulukbek-Toichuev/loadhound/actions/workflows/go.yml/badge.svg)

> A fast, lightweight CLI tool for load testing SQL-based databases with flexible configuration and built-in random data generators.

## Quick Start

```bash
# Download and install (replace with actual installation method)
go install github.com/Ulukbek-Toichuev/loadhound@latest

# Run a load test
loadhound --run-test my-test-scenario.toml
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

## Example Scenario

```toml
[db]
driver="postgres"
dsn="postgres://postgres:passwd@localhost:5432/loadhound_db?sslmode=disable"

[db.conn_pool]
max_open_connections=2
max_idle_connections=2
conn_max_idle_time="1m"
conn_max_lifetime="1m"

[workflow]
[[workflow.scenarios]]
name="select_scenario"
duration="15s"
threads=2
pacing="1s"

[workflow.scenarios.statement]
name="select"
query="select * from loadhound_table lt where lt.rand_bool = $1 and lt.rand_int = $2;"
args="randBool, randIntRange 100 1000"

[output.report]
to_file=true
to_console=true

[output.log]
to_file= false
to_console=true
level="debug"
```

## Usage

### CLI Flags

Flag | Description
-----|------------
`--run-test` | Path to your `.toml` scenario file
`-version` | Print LoadHound version

### Built-in parameter functions

Function | Description | Return type
---------|-------------|-------------
`randBool` | Returns `true` or `false` | `bool`
`randIntRange(a, b)` | Random integer in range | `int`
`randFloat64InRange(a, b)` | Random float in range | `float64`
`randUUID` | Random UUID string | `string`
`randStrRange(a, b)` | Random string of given length | `string`
`getTimestampNow` | Current timestamp | `int`

### Logs

- Logs can be saved in file with name: `loadhound_2006-01-02T15:04:05Z07:00.log`
- LoadHound supports global log levels:`panic fatal error warn info debug trace`

### Report

- Report can be saved in .json file with name: `loadhound_2006-01-02T15:04:05Z07:00.json`
- Report contains your .toml configuration and report

- Report format in `stdout`:

```bash
========== LoadHound Report ==========
duration: 5.230952s

Query
total: 100  failed: 0  qps: 19.12  affected rows: 11100
min: 56.683917ms  max: 442.162417ms
p50: 71.248791ms  p90: 125.995541ms  p95: 185.654099ms

Iteration
total: 100

Thread
total: 5

Errors
No errors recorded.
```

- Report format in `JSON`:

```json
{
  "test_config": {
    "db": {
      "driver": "postgres",
      "dsn": "postgres://postgres:passwd@localhost:5432/loadhound_db?sslmode=disable",
      "conn_pool": {
        "max_open_connections": 2,
        "max_idle_connections": 2,
        "conn_max_idle_time": "1m",
        "conn_max_life_time": "1m"
      }
    },
    "workflow": {
      "scenarios": [
        {
          "duration": "15s",
          "pacing": "1s",
          "ramp_up": "0s",
          "name": "select_scenario",
          "iterations": 0,
          "threads": 2,
          "statement": {
            "name": "select",
            "query": "select * from loadhound_table lt where lt.rand_bool = $1 and lt.rand_int = $2;\n",
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
        "level": "debug",
        "to_file": false,
        "to_console": true
      }
    }
  },
  "test_duration": "15.148043708s",
  "query_data": {
    "total": 30,
    "qps": "2.00",
    "min": "41.465625ms",
    "max": "257.656041ms",
    "p50": "46.757021ms",
    "p90": "58.230266ms",
    "p95": "63.17277ms",
    "affected_rows": 69,
    "err_total": 0
  },
  "iteration_data": {
    "total": 30
  },
  "thread_data": {
    "total": 2
  },
  "top_errors": []
}
```
  
## Supported Databases

Databases | Driver
----------|-----------
MySQL | github.com/go-sql-driver/mysql
PostgreSQL | github.com/lib/pq

## License

LoadHound is licensed under the [MIT License](LICENSE).
