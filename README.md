# LoadHound

![Go Version](https://img.shields.io/badge/Go-1.21+-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Status](https://img.shields.io/badge/status-alpha-orange)

## Table of Contents

- [Description](#description)
- [Features](#features)
- [Example Scenario](#example-scenario)
- [Usage](#usage)
- [Supported Databases](#supported-databases)
- [License](#license)

## Description

**LoadHound** is a fast, lightweight CLI tool for load testing SQL-based databases.
Define flexible, repeatable test scenarios using a simple TOML configuration format — no bloated GUI, just pure load.

Simple, flexible, and built for performance.

## Features

- Easy-to-write test scenarios in **TOML**
- Built-in **random data generators**
- Support for:
  - PostgreSQL
  - MySQL
- Flexible load configuration:
  - `duration`, `iterations`, `threads`, `pacing`, `ramp_up`
- Supports **prepared statements** and **parameterized queries**
- Adjustable **connection pooling**
- Output to console and/or file
- Support log levels

## Example Scenario

```toml
[db]
driver="postgres"
dsn="postgres://postgres:passwd@localhost:5432/loadhound_db?sslmode=disable"

[db.conn_pool]
max_open_connections=5
max_idle_connections=2
conn_max_idle_time="30s"
conn_max_lifetime="1m"

[workflow]
# Scenario 1
[[workflow.scenarios]]
name="scenario_insert"
duration="10m"
threads=2
pacing="500ms"
ramp_up="1m"

[workflow.scenarios.statement]
name="insert_tasks"
query="INSERT INTO tasks (title, description, priority) VALUES ($1, $2, $3);"
args="randStrRange 10 20, randStrRange 50 100, randIntRange 1 2"

# Scenario 2
[[workflow.scenarios]]
name="scenario_select"
duration="10m"
threads=10
pacing="500ms"
ramp_up="1m"

[workflow.scenarios.statement]
name="select_tasks"
query="SELECT * FROM tasks t WHERE t.priority = $1;"
args="randIntRange 1 2"

[output.report]
to_file=true
to_console=true

[output.log]
to_file= true
to_console=false
level="trace"
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
        "conn_max_idle_time": 0,
        "conn_max_life_time": 0
      }
    },
    "workflow": {
      "scenarios": [
        {
          "name": "select_scenario",
          "iterations": 20,
          "duration": 0,
          "threads": 5,
          "pacing": 250000000,
          "ramp_up": 0,
          "statement": {
            "name": "select_users",
            "query": "SELECT * FROM users;",
            "args": ""
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
        "level": "trace",
        "to_file": true,
        "to_console": true
      }
    }
  },
  "test_duration": "5.230952s",
  "query_data": {
    "total": 100,
    "qps": "19.12",
    "min": "56.683917ms",
    "max": "442.162417ms",
    "p50": "71.248791ms",
    "p90": "125.995541ms",
    "p95": "185.654099ms",
    "affected_rows": 11100,
    "err_total": 0
  },
  "iteration_data": {
    "total": 100
  },
  "thread_data": {
    "total": 5
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
