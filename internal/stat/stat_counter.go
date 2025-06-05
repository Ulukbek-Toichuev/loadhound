/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package stat

import (
	"fmt"
)

// func PrintCurrStat(workerId, iterNum int, queryStat *QueryStat) {
// 	fmt.Printf("[Worker %d] Iter: %d | Latency: %dms | Rows: %d\n", workerId+1, iterNum, queryStat.Latency.Milliseconds(), queryStat.AffectedRows)
// }

func PrintSummary(st *SummaryStat) {
	fmt.Printf("\nLoad Test Summary:\n")
	fmt.Printf("------------------------\n")
	fmt.Printf("test start time: %s\ntest end time: %s\nmax response time: %d ms\nmin response time: %d ms\nsuccess queries count: %d\nfailed queries count: %d\ntotal queries count: %d\nworkers count: %d\niteration count: %d\n",
		st.TestStart.String(), st.TestEnd.String(), st.TotalStat.Max_latency.Milliseconds(), st.TotalStat.Min_latency.Milliseconds(), st.TotalStat.Success, st.TotalStat.Failed, st.TotalStat.Total, st.WorkersCount, st.Iterations)
}
