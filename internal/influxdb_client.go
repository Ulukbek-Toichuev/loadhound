package internal

import (
	"context"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

type InfluxDBClient struct {
	Config       *InfluxDBConfig
	Client       influxdb2.Client
	WriteApi     api.WriteAPIBlocking
	Period       time.Duration
	MetricEngine *MetricEngine
}

func NewInfluxDBClient(influxCfg *InfluxDBConfig, me *MetricEngine) *InfluxDBClient {
	var t time.Duration
	if influxCfg.Period == 0 {
		t = 5 * time.Second
	} else {
		t = influxCfg.Period
	}
	client := influxdb2.NewClient(influxCfg.Url, influxCfg.Token)
	writeAPI := client.WriteAPIBlocking(influxCfg.Org, influxCfg.Bucket)
	return &InfluxDBClient{
		Config:       influxCfg,
		Client:       client,
		WriteApi:     writeAPI,
		Period:       t,
		MetricEngine: me,
	}
}

func (ic *InfluxDBClient) Activate(globalCtx context.Context, wg *sync.WaitGroup, genEvent *GeneralEventController) {
	ticker := time.NewTicker(ic.Period)
	go func(wg *sync.WaitGroup, globalCtx context.Context) {
		defer ic.Client.Close()
		defer wg.Done()
		for {
			select {
			case <-globalCtx.Done():
				return
			case <-ticker.C:
				point := write.NewPoint("query_response",
					map[string]string{
						"unit": "ms",
					},
					map[string]interface{}{
						"max": ic.MetricEngine.respMax.Load(),
					}, time.Now())
				if err := ic.WriteApi.WritePoint(context.Background(), point); err != nil {
					genEvent.WriteErrMsg("failed to send metric to influx", err)
				}
			}
		}
	}(wg, globalCtx)
}
