/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package stat

import (
	"fmt"
	"sync"

	"github.com/Ulukbek-Toichuev/loadhound/internal/db"
)

func PrintCurrStat(workerId, iterNum int, queryStat *db.QueryStat) {
	fmt.Printf("[Worker %d] Iter: %d | Latency: %dms | Rows: %d\n", workerId+1, iterNum, queryStat.Latency.Milliseconds(), queryStat.AffectedRows)
}

func PrintSummary(st *Stat) {
	fmt.Printf("\nLoad Test Summary:\n")
	fmt.Printf("------------------------\n")
	fmt.Printf("max: %d ms\nmin: %d ms\nsuccess: %d\nfailed: %d\ntotal: %d\n", st.MaxValue(), st.MinValue(), st.SuccessValue(), st.FailedValue(), st.TotalValue())
}

func CollectStat(st *Stat, queryStat *db.QueryStat, queryErr error) {
	var mu sync.Mutex
	if queryErr != nil {
		st.IncrementFailed()
	} else {
		st.IncrementSuccess()
		mu.Lock()
		currLat := queryStat.Latency.Milliseconds()
		if currLat < st.MinValue() || st.MinValue() == 0 {
			st.SetMin(currLat)
		} else if currLat > st.MaxValue() {
			st.SetMax(currLat)
		}
		mu.Unlock()
	}
	st.IncrementTotal()
}
