package internal

import (
	"context"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

type InfluxDBClient struct {
	Config   *InfluxDBConfig
	Client   influxdb2.Client
	WriteApi api.WriteAPIBlocking
	Period   time.Duration
}

func NewInfluxDBClient(influxCfg *InfluxDBConfig) *InfluxDBClient {
	var t time.Duration
	if influxCfg.Period == 0 {
		t = 5 * time.Second
	} else {
		t = influxCfg.Period
	}
	client := influxdb2.NewClient(influxCfg.Url, influxCfg.Token)
	writeAPI := client.WriteAPIBlocking(influxCfg.Org, influxCfg.Bucket)
	return &InfluxDBClient{
		Config:   influxCfg,
		Client:   client,
		WriteApi: writeAPI,
		Period:   t,
	}
}

func (ic *InfluxDBClient) Activate(globalCtx context.Context, wg *sync.WaitGroup) {
	ticker := time.NewTicker(ic.Period)
	go func(wg *sync.WaitGroup, globalCtx context.Context) {
		defer ic.Client.Close()
		defer wg.Done()
		for {
			select {
			case <-globalCtx.Done():
				return
			case <-ticker.C:
				// TODO
			}
		}
	}(wg, globalCtx)
}
