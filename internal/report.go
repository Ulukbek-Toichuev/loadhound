/*
LoadHound — Relentless load testing tool for SQL databases.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/fatih/color"
)

type ReportData struct {
	RunConfig    RunConfig  `json:"test_config"`
	TestDuration string     `json:"test_duration"`
	QueryData    QueryData  `json:"query_data"`
	ThreadData   ThreadData `json:"thread_data"`
}

type QueryData struct {
	TotalCount        int64    `json:"queries_total"`
	QPS               string   `json:"qps"`
	RespMin           string   `json:"min_resp_time"`
	RespMax           string   `json:"max_resp_time"`
	SuccessRate       string   `json:"success_rate"`
	FailedRate        string   `json:"failed_rate"`
	P50               string   `json:"p50_resp_time"`
	P90               string   `json:"p90_resp_time"`
	P95               string   `json:"p95_resp_time"`
	RowsAffectedTotal int64    `json:"affected_rows"`
	ErrCount          int64    `json:"err_total"`
	TopErrors         []string `json:"top_errors"`
}

type ThreadData struct {
	ThreadCount    int64 `json:"thread_count"`
	IterationCount int64 `json:"iteration_count"`
}

func GenerateReport(cfg *RunConfig, scenariosMetrics []*Metric) error {
	if cfg.OutputConfig == nil || cfg.OutputConfig.ReportConfig == nil {
		return nil
	}
	calculateReports(cfg, scenariosMetrics)
	reportCfg := cfg.OutputConfig.ReportConfig
	if reportCfg.ToConsole {
		printColorReport(cfg)
	}

	if reportCfg.ToFile {
		filename := fmt.Sprintf("loadhound_report_%s.json", time.Now().Format(time.RFC3339))
		// #nosec G304 -- filename is generated internally, not from user input
		f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}

		data, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return err
		}

		if _, err := f.Write(data); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

func calculateReports(cfg *RunConfig, scenariosMetrics []*Metric) {
	scenariosCfg := cfg.WorkflowConfig.Scenarios
	for idx, sc := range scenariosMetrics {
		// Get percentiles
		respMin, respMax := sc.Td.Quantile(0.00), sc.Td.Quantile(1)
		p50, p90, p95 := sc.Td.Quantile(0.50), sc.Td.Quantile(0.90), sc.Td.Quantile(0.95)

		scenariosCfg[idx].Report = &Report{
			Duration:          sc.StopTime.Sub(sc.StartTime).String(),
			ThreadsTotal:      sc.ThreadsTotal,
			IterationsTotal:   sc.IterationsTotal,
			QueriesTotal:      sc.QueriesTotal,
			QPS:               fmt.Sprintf("%.2f", sc.GetQPS()),
			RespMin:           time.Duration(respMin).String(),
			RespMax:           time.Duration(respMax).String(),
			SuccessRate:       fmt.Sprintf("%.2f%%", sc.GetSuccessRate()),
			FailedRate:        fmt.Sprintf("%.2f%%", sc.GetFailedRate()),
			P50:               time.Duration(p50).String(),
			P90:               time.Duration(p90).String(),
			P95:               time.Duration(p95).String(),
			RowsAffectedTotal: sc.RowsAffected,
			ErrCount:          sc.ErrorsTotal,
			TopErrors:         getTopErrors(sc.ErrMap),
		}
	}
}

func printColorReport(cfg *RunConfig) {
	scenariosCfg := cfg.WorkflowConfig.Scenarios

	bold := color.New(color.Bold).SprintFunc()
	green := color.New(color.FgGreen).SprintFunc()
	cyan := color.New(color.FgCyan).SprintFunc()

	fmt.Print(bold("\n========== LoadHound Report ==========\n"))
	for _, sc := range scenariosCfg {
		report := sc.Report
		fmt.Println()
		fmt.Println(bold(fmt.Sprintf("Name: %s", sc.Name)))

		fmt.Printf("duration: %s\n", cyan(report.Duration))

		fmt.Printf("queries total: %s success_rate: %s failed_rate: %s\n",
			cyan(report.QueriesTotal),
			cyan(report.SuccessRate),
			cyan(report.FailedRate))

		fmt.Printf("qps: %s affected rows: %s\n",
			cyan(report.QPS),
			cyan(report.RowsAffectedTotal))

		fmt.Printf("response time - min: %s  max: %s\n",
			cyan(report.RespMin),
			cyan(report.RespMax))
		fmt.Printf("response time - p50: %s  p90: %s  p95: %s\n",
			cyan(report.P50),
			cyan(report.P90),
			cyan(report.P95))
		fmt.Println()

		fmt.Println(bold("Thread"))
		fmt.Printf("thread count: %s\n", cyan(report.ThreadsTotal))
		fmt.Printf("iteration count: %s\n", cyan(report.IterationsTotal))
		fmt.Println()

		fmt.Println(bold("Errors"))
		fmt.Printf("errors count: %s\n", cyan(report.ErrCount))
		if len(report.TopErrors) == 0 {
			fmt.Println(green("No errors recorded."))
		} else {
			for idx, err := range report.TopErrors {
				fmt.Printf("%d. %s\n", idx+1, err)
			}
		}
	}
}

type errKV struct {
	key   string
	value int64
}

// Get top 5 errors by count
func getTopErrors(errMap map[string]int64) []string {
	const maxErrLen = 5

	// Mapping to slice for sorting
	errKVs := make([]errKV, 0, len(errMap))
	for k, v := range errMap {
		errKVs = append(errKVs, errKV{k, v})
	}

	// Sorting by desc
	sort.Slice(errKVs, func(i, j int) bool {
		return errKVs[i].value > errKVs[j].value
	})

	n := len(errKVs)
	if n > maxErrLen {
		n = maxErrLen
	}

	errSlice := make([]string, 0, n)
	for i := 0; i < n; i++ {
		errSlice = append(errSlice, errKVs[i].key)
	}

	return errSlice
}
