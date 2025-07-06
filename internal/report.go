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
	"sync"
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
	TotalCount   int64  `json:"total"`
	QPS          string `json:"qps"`
	RespMin      string `json:"min"`
	RespMax      string `json:"max"`
	P50          string `json:"p50"`
	P90          string `json:"p90"`
	P95          string `json:"p95"`
	AffectedRows int64  `json:"affected_rows"`
	ErrCount     int64  `json:"err_total"`
}

type IterationData struct {
	TotalCount int64 `json:"total"`
}

type ThreadData struct {
	TotalCount  int64 `json:"total"`
	FailedCount int64 `json:"failed"`
}

func getReportData(cfg *RunTestConfig, metricEngine *MetricEngine) *ReportData {
	totalQuery := metricEngine.queryTotal.Load()
	qps := metricEngine.qps.Load()
	respMin, respMax := metricEngine.respMin.Load(), metricEngine.respMax.Load()

	// Get percentiles
	p50, p90, p95 := metricEngine.td.td.Quantile(0.50), metricEngine.td.td.Quantile(0.90), metricEngine.td.td.Quantile(0.95)
	errTotal := metricEngine.errTotal.Load()

	return &ReportData{
		RunTestConfig: cfg,
		TestDuration:  time.Since(metricEngine.start.Load()).String(),
		QueryData: &QueryData{
			TotalCount:   totalQuery,
			QPS:          fmt.Sprintf("%.2f", qps),
			RespMin:      respMin.String(),
			RespMax:      respMax.String(),
			P50:          time.Duration(p50).String(),
			P90:          time.Duration(p90).String(),
			P95:          time.Duration(p95).String(),
			AffectedRows: metricEngine.affectedRowsTotal.Load(),
			ErrCount:     errTotal,
		},
		IterationData: &IterationData{
			TotalCount: metricEngine.iterTotal.Load(),
		},
		ThreadData: &ThreadData{
			TotalCount:  metricEngine.threadsTotal.Load(),
			FailedCount: metricEngine.threadsFailed.Load(),
		},
		TopErrors: getTopErrors(&metricEngine.errMap),
	}
}

func GenerateReport(cfg *RunTestConfig, metricEngine *MetricEngine) error {
	if cfg.OutputConfig == nil {
		return nil
	}

	if cfg.OutputConfig.ReportConfig == nil {
		return nil
	}

	report := getReportData(cfg, metricEngine)
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
	fmt.Printf("type: %s  duration: %s\n", cyan(report.RunTestConfig.WorkflowConfig.Type), cyan(report.TestDuration))
	fmt.Println()

	fmt.Println(bold("Query"))
	fmt.Printf("total: %s  failed: %s  qps: %s  affected rows: %s\n", cyan(report.QueryData.TotalCount), cyan(report.QueryData.ErrCount), cyan(report.QueryData.QPS), cyan(report.QueryData.AffectedRows))
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
func getTopErrors(errSyncMap *sync.Map) []string {
	const maxErrLen = 5

	errMap := make(map[string]int)

	// Get data from sync.Map to errMap
	errSyncMap.Range(func(key, value any) bool {
		errMap[key.(string)] = value.(int)
		return true
	})

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
