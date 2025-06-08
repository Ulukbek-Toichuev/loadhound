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

func PrintSummary(st *SummaryStat) {
	fmt.Printf("\nLoad Test Summary:\n")
	fmt.Printf("------------------------\n")
	fmt.Printf("test start time: %s\ntest end time: %s\nmax response time: %d ms\nmin response time: %d ms\nsuccess queries count: %d\nfailed queries count: %d\ntotal queries count: %d\nworkers count: %d\niteration count: %d\n",
		st.TestStart.String(), st.TestEnd.String(), st.TotalStat.Max_latency.Milliseconds(), st.TotalStat.Min_latency.Milliseconds(), st.TotalStat.Success, st.TotalStat.Failed, st.TotalStat.Total, st.WorkersCount, st.Iterations)
}

func SaveInFile(st *SummaryStat, file string) {
	b, err := json.MarshalIndent(st, "", "  ") // <-- вот тут отличие
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
