package internal

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func BenchmarkAdd(b *testing.B) {
	me := NewMetricEngine(1000)
	for i := 0; i < 10; i++ {
		qm := &QueryMetric{
			Query:        "SELECT * FROM tableA",
			ResponseTime: time.Duration(RandIntRange(150, 500) * int(time.Millisecond)),
			AffectedRows: rand.Int64N(10000),
			Err:          fmt.Errorf("error from thread-%d", i),
			ThreadID:     i,
		}
		name := fmt.Sprintf("QueryMetric from thread-id: %d", i)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				me.AddQueryMetric(qm)
			}
		})
	}
}

type MetricEngineAssert struct {
	queryTotal int64
	errTotal   int64
	RespMin    time.Duration
	RespMax    time.Duration
}

func TestMetricEngine_Add(t *testing.T) {
	var me = NewMetricEngine(100)
	var testCases = []struct {
		name string
		qm   *QueryMetric
		ma   *MetricEngineAssert
	}{
		{
			"QueryMetric without err",
			&QueryMetric{"select * from tableA", time.Duration(100 * time.Millisecond), 1, nil, 1},
			&MetricEngineAssert{1, 0, time.Duration(100 * time.Millisecond), time.Duration(100 * time.Millisecond)},
		},
		{
			"QueryMetric with err",
			&QueryMetric{"select * from tableA", time.Duration(50 * time.Millisecond), 1, fmt.Errorf("Some error"), 1},
			&MetricEngineAssert{2, 1, time.Duration(50 * time.Millisecond), time.Duration(100 * time.Millisecond)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			me.AddQueryMetric(tc.qm)
			assert.Equal(t, tc.ma.queryTotal, me.queryTotal.Load())
			assert.Equal(t, tc.ma.errTotal, me.errTotal.Load())
			assert.Equal(t, tc.ma.RespMin, me.respMin.Load())
			assert.Equal(t, tc.ma.RespMax, me.respMax.Load())
		})
	}
}
