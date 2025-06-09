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
	"time"
)

func PrintPretty(r *Result) {
	var result strings.Builder
	result.WriteString("\n\n")
	result.WriteString("\nTest summary\n")
	result.WriteString("──────────────────────────────\n")
	result.WriteString(fmt.Sprintf("Start Time:       %s\n", r.Start.Format(time.RFC3339)))
	result.WriteString(fmt.Sprintf("End Time:         %s\n", r.End.Format(time.RFC3339)))
	result.WriteString(fmt.Sprintf("Duration:         %s\n", r.End.Sub(r.Start).Truncate(time.Millisecond)))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("Total Queries:    %d\n", r.TotalQueries))
	result.WriteString(fmt.Sprintf("Successful:       %d\n", r.SuccessQueries))
	result.WriteString(fmt.Sprintf("Failed:           %d\n", r.FailedQueries))
	result.WriteString(fmt.Sprintf("Throughput:       %0.2f QPS\n", r.Throughput))
	result.WriteString("\n")

	result.WriteString("Latency (ms)\n")
	result.WriteString("──────────────────────────────\n")
	result.WriteString(fmt.Sprintf("Min:              %d ms\n", r.Latency.Min))
	result.WriteString(fmt.Sprintf("Max:              %d ms\n", r.Latency.Max))
	result.WriteString(fmt.Sprintf("Avg:              %.2f ms\n", r.Latency.Avg))
	result.WriteString(fmt.Sprintf("Median:           %d ms\n", r.Latency.Median))
	result.WriteString(fmt.Sprintf("P90:              %d ms\n", r.Latency.P90))
	result.WriteString(fmt.Sprintf("P95:              %d ms\n", r.Latency.P95))
	result.WriteString(fmt.Sprintf("P99:              %d ms\n", r.Latency.P99))
	result.WriteString("\n")

	if len(r.TopErrors) > 0 {
		result.WriteString("Top Errors\n")
		result.WriteString("──────────────────────────────\n")
		for i, err := range r.TopErrors {
			result.WriteString(fmt.Sprintf("%d. %s\n", i+1, err))
		}
	} else {
		result.WriteString("No errors encountered.\n")
	}

	fmt.Println(result.String())
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
