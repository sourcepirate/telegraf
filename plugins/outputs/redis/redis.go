package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/serializers"
)

var (
	connectionError = errors.New("Connection Failed")
)

type RedisPlugin struct {
	Prefix     string                  `toml:"prefix"`
	Password   string                  `toml:"password"`
	Host       string                  `toml:"host"`
	client     *redistimeseries.Client `toml:"_"`
	Log        telegraf.Logger         `toml:"_"`
	serializer serializers.Serializer  `toml:"_"`
}

var sampleConfig = `
## Redis Prefix
prefix = ""
## Redis Password
password = ""
## Redis Host
host = """
`

func getValue(v interface{}) (bool, float64) {
	switch i := v.(type) {
	case string:
		return false, 0
	case float64:
		// The payload will be encoded as JSON, which does not allow NaN or Inf.
		return !math.IsNaN(i) && !math.IsInf(i, 0), float64(i)
	case uint64:
		return true, float64(i)
	}
	return false, 0
}

// SampleConfig
func (r *RedisPlugin) SampleConfig() string {
	return sampleConfig
}

// Description
func (r *RedisPlugin) Description() string {
	return "Redis Output plugin"
}

// Connect
func (r *RedisPlugin) Connect() error {
	r.client = redistimeseries.NewClient(r.Host, r.Prefix, nil)
	return nil
}

// Close
func (r *RedisPlugin) Close() error {
	return r.client.Pool.Close()
}

// Write
func (r *RedisPlugin) Write(metrics []telegraf.Metric) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()
	return r.writeWithTimeout(ctx, metrics)
}

func (r *RedisPlugin) writeWithTimeout(ctx context.Context, metrics []telegraf.Metric) error {
	defer ctx.Done()
	for _, metric := range metrics {

		for key, value := range metric.Fields() {
			flag, val := getValue(value)
			metricName := metric.Name() + "." + key
			if flag {
				r.createMetricIfNotExist(metric, metricName)
				storedStamp, err := r.client.Add(metricName, metric.Time().Unix(), val)
				fmt.Println(metricName, storedStamp)
				if err != nil {
					r.Log.Error(err)
				}
			}
		}
	}
	return nil
}

func (r *RedisPlugin) getLabels(metric telegraf.Metric) map[string]string {
	var tagList map[string]string = make(map[string]string)
	for _, tag := range metric.TagList() {
		tagList[tag.Key] = tag.Value
	}
	return tagList
}

func (r *RedisPlugin) createMetricIfNotExist(metric telegraf.Metric, keyname string) {
	options := redistimeseries.DefaultCreateOptions
	options.Labels = r.getLabels(metric)
	_, ok := r.client.Info(keyname)
	if ok != nil {
		r.client.CreateKeyWithOptions(keyname, options)
		r.client.CreateKeyWithOptions(keyname+"_avg", options)
		r.client.CreateRule(keyname, redistimeseries.AvgAggregation, 60, keyname+"_avg")
	}
}

func init() {
	outputs.Add("redis", func() telegraf.Output {
		return &RedisPlugin{}
	})
}
