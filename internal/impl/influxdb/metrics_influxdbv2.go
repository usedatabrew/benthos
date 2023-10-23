package influxdb

import (
	"context"
	"fmt"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"net/http"
	"time"

	client "github.com/influxdata/influxdb-client-go/v2"
	"github.com/rcrowley/go-metrics"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	imetrics "github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	_ = bundle.AllMetrics.Add(newInfluxDBV2, docs.ComponentSpec{
		Name:        "influxdbv2",
		Type:        docs.TypeMetrics,
		Status:      docs.StatusBeta,
		Version:     "3.36.0",
		Summary:     `Send metrics to InfluxDB 1.x using the ` + "`/write`" + ` endpoint.`,
		Description: `See https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint for further details on the write API.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldURL("url", "A URL of the format `[https|http|udp]://host:port` to the InfluxDB host.").HasDefault(""),
			docs.FieldString("bucket", "The name of the bucket to use."),
			btls.FieldSpec(),
			docs.FieldString("batch_size", "Size of the batch to group metrics"),
			docs.FieldString("org", "Name of the organisation").Advanced().HasDefault(""),
			docs.FieldString("token", "A token for your org").Advanced().HasDefault("").Secret(),
			docs.FieldObject("include", "Optional additional metrics to collect, enabling these metrics may have some performance implications as it acquires a global semaphore and does `stoptheworld()`.").WithChildren(
				docs.FieldString("runtime", "A duration string indicating how often to poll and collect runtime metrics. Leave empty to disable this metric", "1m").HasDefault(""),
				docs.FieldString("debug_gc", "A duration string indicating how often to poll and collect GC metrics. Leave empty to disable this metric.", "1m").HasDefault(""),
			).Advanced(),
			docs.FieldString("interval", "A duration string indicating how often metrics should be flushed.").Advanced().HasDefault("1m"),
			docs.FieldString("ping_interval", "A duration string indicating how often to ping the host.").Advanced().HasDefault("20s"),
			docs.FieldString("precision", "[ns|us|ms|s] timestamp precision passed to write api.").Advanced().HasDefault("s"),
			docs.FieldString("timeout", "How long to wait for response for both ping and writing metrics.").Advanced().HasDefault("5s"),
			docs.FieldString("tags", "Global tags added to each metric.",
				map[string]string{
					"hostname": "localhost",
					"zone":     "danger",
				},
			).Map().Advanced().HasDefault(map[string]any{}),
			docs.FieldString("retention_policy", "Sets the retention policy for each write.").Advanced().HasDefault(""),
			docs.FieldString("write_consistency", "[any|one|quorum|all] sets write consistency when available.").Advanced().HasDefault(""),
		),
	})
}

type influxDBV2Metrics struct {
	client   client.Client
	writeApi api.WriteAPI

	interval     time.Duration
	pingInterval time.Duration
	timeout      time.Duration

	ctx    context.Context
	cancel func()

	registry        metrics.Registry
	runtimeRegistry metrics.Registry
	config          imetrics.InfluxDBV2Config
	mgr             bundle.NewManagement
	log             log.Modular
}

func newInfluxDBV2(config imetrics.Config, nm bundle.NewManagement) (imetrics.Type, error) {
	i := &influxDBV2Metrics{
		config:          config.InfluxDBV2,
		registry:        metrics.NewRegistry(),
		runtimeRegistry: metrics.NewRegistry(),
		mgr:             nm,
		log:             nm.Logger(),
	}

	i.ctx, i.cancel = context.WithCancel(context.Background())

	if config.InfluxDB.Include.Runtime != "" {
		metrics.RegisterRuntimeMemStats(i.runtimeRegistry)
		interval, err := time.ParseDuration(config.InfluxDB.Include.Runtime)
		if err != nil {
			return nil, fmt.Errorf("failed to parse interval: %s", err)
		}
		go metrics.CaptureRuntimeMemStats(i.runtimeRegistry, interval)
	}

	if config.InfluxDB.Include.DebugGC != "" {
		metrics.RegisterDebugGCStats(i.runtimeRegistry)
		interval, err := time.ParseDuration(config.InfluxDB.Include.DebugGC)
		if err != nil {
			return nil, fmt.Errorf("failed to parse interval: %s", err)
		}
		go metrics.CaptureDebugGCStats(i.runtimeRegistry, interval)
	}

	var err error
	if i.interval, err = time.ParseDuration(config.InfluxDB.Interval); err != nil {
		return nil, fmt.Errorf("failed to parse interval: %s", err)
	}

	if i.pingInterval, err = time.ParseDuration(config.InfluxDB.PingInterval); err != nil {
		return nil, fmt.Errorf("failed to parse ping interval: %s", err)
	}

	if i.timeout, err = time.ParseDuration(config.InfluxDB.Timeout); err != nil {
		return nil, fmt.Errorf("failed to parse timeout interval: %s", err)
	}

	if err := i.makeClient(); err != nil {
		return nil, err
	}

	go i.loop()

	return i, nil
}

func (i *influxDBV2Metrics) makeClient() error {
	var c client.Client
	c = client.NewClientWithOptions(i.config.URL, i.config.Token,
		client.DefaultOptions().SetBatchSize(20))

	i.writeApi = c.WriteAPI(i.config.Organisation, i.config.Bucket)
	i.client = c

	return nil
}

func (i *influxDBV2Metrics) loop() {
	ticker := time.NewTicker(i.interval)
	pingTicker := time.NewTicker(i.pingInterval)
	defer ticker.Stop()
	defer pingTicker.Stop()
	for {
		select {
		case <-i.ctx.Done():
			return
		case <-ticker.C:
			if err := i.publishRegistry(); err != nil {
				i.log.Errorf("failed to send metrics data: %s", err)
			}
		case <-pingTicker.C:
			_, err := i.client.Ping(context.TODO())
			if err != nil {
				i.log.Warnf("unable to ping influx endpoint: %s", err)
				if err = i.makeClient(); err != nil {
					i.log.Errorf("unable to recreate client: %s", err)
				}
			}
		}
	}
}

func (i *influxDBV2Metrics) publishRegistry() error {
	now := time.Now()
	all := i.getAllMetrics()
	for k, v := range all {
		name, normalTags := decodeInfluxDBName(k)
		tags := make(map[string]string, len(i.config.Tags)+len(normalTags))
		// apply normal tags
		for k, v := range normalTags {
			tags[k] = v
		}
		// override with any global
		for k, v := range i.config.Tags {
			tags[k] = v
		}
		p := client.NewPoint(name, tags, v, now)
		i.writeApi.WritePoint(p)
	}

	i.writeApi.Flush()
	return nil
}

func (i *influxDBV2Metrics) getAllMetrics() map[string]map[string]any {
	data := make(map[string]map[string]any)
	i.registry.Each(func(name string, metric any) {
		values := getMetricValues(metric)
		data[name] = values
	})
	i.runtimeRegistry.Each(func(name string, metric any) {
		values := getMetricValues(metric)
		data[name] = values
	})
	return data
}

func (i *influxDBV2Metrics) GetCounter(path string) imetrics.StatCounter {
	encodedName := encodeInfluxDBName(path, nil, nil)
	return i.registry.GetOrRegister(encodedName, func() metrics.Counter {
		return influxDBCounter{
			metrics.NewCounter(),
		}
	}).(influxDBCounter)
}

func (i *influxDBV2Metrics) GetCounterVec(path string, n ...string) imetrics.StatCounterVec {
	return imetrics.FakeCounterVec(func(l ...string) imetrics.StatCounter {
		encodedName := encodeInfluxDBName(path, n, l)
		return i.registry.GetOrRegister(encodedName, func() metrics.Counter {
			return influxDBCounter{
				metrics.NewCounter(),
			}
		}).(influxDBCounter)
	})
}

func (i *influxDBV2Metrics) GetTimer(path string) imetrics.StatTimer {
	encodedName := encodeInfluxDBName(path, nil, nil)
	return i.registry.GetOrRegister(encodedName, func() metrics.Timer {
		return influxDBTimer{
			metrics.NewTimer(),
		}
	}).(influxDBTimer)
}

func (i *influxDBV2Metrics) GetTimerVec(path string, n ...string) imetrics.StatTimerVec {
	return imetrics.FakeTimerVec(func(l ...string) imetrics.StatTimer {
		encodedName := encodeInfluxDBName(path, n, l)
		return i.registry.GetOrRegister(encodedName, func() metrics.Timer {
			return influxDBTimer{
				metrics.NewTimer(),
			}
		}).(influxDBTimer)
	})
}

func (i *influxDBV2Metrics) GetGauge(path string) imetrics.StatGauge {
	encodedName := encodeInfluxDBName(path, nil, nil)
	result := i.registry.GetOrRegister(encodedName, func() metrics.Gauge {
		return influxDBGauge{
			metrics.NewGauge(),
		}
	}).(influxDBGauge)
	return result
}

func (i *influxDBV2Metrics) GetGaugeVec(path string, n ...string) imetrics.StatGaugeVec {
	return imetrics.FakeGaugeVec(func(l ...string) imetrics.StatGauge {
		encodedName := encodeInfluxDBName(path, n, l)
		return i.registry.GetOrRegister(encodedName, func() metrics.Gauge {
			return influxDBGauge{
				metrics.NewGauge(),
			}
		}).(influxDBGauge)
	})
}

func (i *influxDBV2Metrics) HandlerFunc() http.HandlerFunc {
	return nil
}

func (i *influxDBV2Metrics) Close() error {
	if err := i.publishRegistry(); err != nil {
		i.log.Errorf("failed to send metrics data: %s", err)
	}
	i.client.Close()
	return nil
}
