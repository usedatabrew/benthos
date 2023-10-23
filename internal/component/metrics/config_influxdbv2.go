package metrics

// InfluxDBV2Config is config for the influx metrics type.
type InfluxDBV2Config struct {
	URL    string `json:"url" yaml:"url"`
	Bucket string `json:"bucket" yaml:"bucket"`

	BatchSize        int             `json:"batch_size" yaml:"batch_size"`
	Interval         string          `json:"interval" yaml:"interval"`
	Token            string          `json:"token" yaml:"token"`
	Organisation     string          `json:"organisation" yaml:"organisation"`
	PingInterval     string          `json:"ping_interval" yaml:"ping_interval"`
	Precision        string          `json:"precision" yaml:"precision"`
	Timeout          string          `json:"timeout" yaml:"timeout"`
	RetentionPolicy  string          `json:"retention_policy" yaml:"retention_policy"`
	WriteConsistency string          `json:"write_consistency" yaml:"write_consistency"`
	Include          InfluxDBInclude `json:"include" yaml:"include"`

	Tags map[string]string `json:"tags" yaml:"tags"`
}

// InfluxDBV2Include contains configuration parameters for optional metrics to
// include.
type InfluxDBV2Include struct {
	Runtime string `json:"runtime" yaml:"runtime"`
	DebugGC string `json:"debug_gc" yaml:"debug_gc"`
}

// NewInfluxDBV2Config creates an InfluxDBV2Config struct with default values.
func NewInfluxDBV2Config() InfluxDBV2Config {
	return InfluxDBV2Config{
		URL:    "",
		Bucket: "",

		Precision:    "s",
		Interval:     "1m",
		PingInterval: "20s",
		Timeout:      "5s",
	}
}
