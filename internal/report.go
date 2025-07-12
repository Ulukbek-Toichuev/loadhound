/*
LoadHound — Relentless SQL load testing tool.
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
	RunTestConfig *RunTestConfig `json:"test_config"`
	TestDuration  string         `json:"test_duration"`
	QueryData     *QueryData     `json:"query_data"`
	IterationData *IterationData `json:"iteration_data"`
	ThreadData    *ThreadData    `json:"thread_data"`
	TopErrors     []string       `json:"top_errors"`
}

type QueryData struct {
	TotalCount        int64  `json:"total"`
	QPS               string `json:"qps"`
	RespMin           string `json:"min"`
	RespMax           string `json:"max"`
	P50               string `json:"p50"`
	P90               string `json:"p90"`
	P95               string `json:"p95"`
	RowsAffectedTotal int64  `json:"affected_rows"`
	ErrCount          int64  `json:"err_total"`
}

type IterationData struct {
	TotalCount int64 `json:"total"`
}

type ThreadData struct {
	TotalCount  int64 `json:"total"`
	FailedCount int64 `json:"failed"`
}

func getReportData(cfg *RunTestConfig, globalMetric *GlobalMetric) *ReportData {
	totalQuery := globalMetric.QueriesTotal
	qpsMax := globalMetric.Qps.Max
	respMin, respMax := globalMetric.RespTime.Min, globalMetric.RespTime.Max

	// Get percentiles
	p50, p90, p95 := globalMetric.Td.Quantile(0.50), globalMetric.Td.Quantile(0.90), globalMetric.Td.Quantile(0.95)
	errTotal := globalMetric.ErrorsTotal

	return &ReportData{
		RunTestConfig: cfg,
		TestDuration:  time.Since(time.Now()).String(),
		QueryData: &QueryData{
			TotalCount:        totalQuery,
			QPS:               fmt.Sprintf("%.2f", qpsMax),
			RespMin:           respMin.String(),
			RespMax:           respMax.String(),
			P50:               time.Duration(p50).String(),
			P90:               time.Duration(p90).String(),
			P95:               time.Duration(p95).String(),
			RowsAffectedTotal: globalMetric.RowsAffectedTotal,
			ErrCount:          errTotal,
		},
		IterationData: &IterationData{
			TotalCount: globalMetric.IterationsTotal,
		},
		ThreadData: &ThreadData{
			TotalCount:  0,
			FailedCount: 0,
		},
		TopErrors: getTopErrors(globalMetric.ErrMap),
	}
}

func GenerateReport(cfg *RunTestConfig, globalMetric *GlobalMetric) error {
	if cfg.OutputConfig == nil {
		return nil
	}

	if cfg.OutputConfig.ReportConfig == nil {
		return nil
	}

	report := getReportData(cfg, globalMetric)
	reportCfg := cfg.OutputConfig.ReportConfig
	if reportCfg.ToConsole {
		printColorReport(report)
	}

	if reportCfg.ToFile {
		f, err := os.OpenFile(getReportFilename(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}

		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}

		f.Write(data)
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

func getReportFilename() string {
	return fmt.Sprintf("loadhound_report_%s.json", time.Now().Format(time.RFC3339))
}

func printColorReport(report *ReportData) {
	bold := color.New(color.Bold).SprintFunc()
	green := color.New(color.FgGreen).SprintFunc()
	cyan := color.New(color.FgCyan).SprintFunc()

	fmt.Print(bold("\n========== LoadHound Report ==========\n"))
	fmt.Printf("duration: %s\n", cyan(report.TestDuration))
	fmt.Println()

	fmt.Println(bold("Query"))
	fmt.Printf("total: %s  failed: %s  qps: %s  affected rows: %s\n", cyan(report.QueryData.TotalCount), cyan(report.QueryData.ErrCount), cyan(report.QueryData.QPS), cyan(report.QueryData.RowsAffectedTotal))
	fmt.Printf("min: %s  max: %s\n", cyan(report.QueryData.RespMin), cyan(report.QueryData.RespMax))
	fmt.Printf("p50: %s  p90: %s  p95: %s\n", cyan(report.QueryData.P50), cyan(report.QueryData.P90), cyan(report.QueryData.P95))
	fmt.Println()

	fmt.Println(bold("Iteration"))
	fmt.Printf("total: %s\n", cyan(report.IterationData.TotalCount))
	fmt.Println()

	fmt.Println(bold("Thread"))
	fmt.Printf("total: %s  failed: %s\n", cyan(report.ThreadData.TotalCount), cyan(report.ThreadData.FailedCount))
	fmt.Println()

	fmt.Println(bold("Errors"))
	if len(report.TopErrors) == 0 {
		fmt.Println(green("No errors recorded."))
	} else {
		for idx, err := range report.TopErrors {
			fmt.Printf("%d. %s\n", idx+1, err)
		}
	}
}

type errKV struct {
	key   string
	value int
}

// Get top 5 errors by count
func getTopErrors(errMap map[string]int) []string {
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
