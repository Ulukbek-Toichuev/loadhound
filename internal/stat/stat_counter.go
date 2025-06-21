/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package stat

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func PrintResultPretty(r *Result) {
	var result strings.Builder

	sections := []string{
		formatHeader(),
		formatMeta(r),
		formatQueryStats(r),
		formatLatency(r),
		formatErrors(r),
	}

	for _, section := range sections {
		result.WriteString(section)
	}

	fmt.Println(result.String())
}

func formatHeader() string {
	return "\n\nTest summary\n──────────────────────────────\n"
}

func formatMeta(r *Result) string {
	return fmt.Sprintf(
		"Start Time:       %s\nEnd Time:         %s\nDuration:         %s\n\n",
		r.Start, r.End, r.TotalTime,
	)
}

func formatQueryStats(r *Result) string {
	return fmt.Sprintf(
		"Total Queries:    %d\nSuccessful:       %d\nFailed:           %d\nThroughput:       %.2f QPS\n\n",
		r.TotalQueries, r.SuccessQueries, r.FailedQueries, r.Throughput,
	)
}

func formatLatency(r *Result) string {
	return fmt.Sprintf(
		"Latency (ms)\n──────────────────────────────\nMin:              %dms\nMax:              %dms\nAvg:              %.2fms\nMedian:           %dms\nP90:              %dms\nP95:              %dms\nP99:              %dms\n\n",
		r.Latency.Min, r.Latency.Max, r.Latency.Avg, r.Latency.Median, r.Latency.P90, r.Latency.P95, r.Latency.P99,
	)
}

func formatErrors(r *Result) string {
	if len(r.TopErrors) == 0 {
		return "No errors encountered.\n"
	}

	var b strings.Builder
	b.WriteString("Top Errors\n──────────────────────────────\n")
	for i, err := range r.TopErrors {
		b.WriteString(fmt.Sprintf("%d. %s\n", i+1, err))
	}
	return b.String()
}

func SaveInFile(r *Result, file string) {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		panic(err)
	}

	if !strings.HasSuffix(file, ".json") {
		file = fmt.Sprintf("%s.json", file)
	}

	err = os.WriteFile(file, b, 0644)
	if err != nil {
		panic(err)
	}
}
