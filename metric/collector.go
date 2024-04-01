package metric

import (
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/robfig/cron/v3"
	"github.com/txix-open/isp-kit/metrics"
)

const (
	EverySecond = "* * * * * *"
)

type Value struct {
	Labels []string
	Value  float64
}

func ValueOf(value int, labels ...string) Value {
	return Value{
		Value:  float64(value),
		Labels: labels,
	}
}

type Metric struct {
	Name        string
	Description string
	Labels      []string
	Collect     func() []Value
}

type Collector struct {
	cronSpec string
	module   string
	closed   chan struct{}
}

func NewCollector(cronSpec string, module string) *Collector {
	return &Collector{
		cronSpec: cronSpec,
		module:   module,
		closed:   make(chan struct{}),
	}
}

func (c *Collector) Add(m Metric) {
	sched, err := cron.NewParser(
		cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	).Parse(c.cronSpec)
	if err != nil {
		panic(errors.WithMessagef(err, "parse %s", c.cronSpec))
	}

	gauge := metrics.GetOrRegister(
		metrics.DefaultRegistry,
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: m.Name,
			Help: m.Description,
		}, append(m.Labels, "module")),
	)

	go func() {
		for {
			for _, value := range m.Collect() {
				gauge.WithLabelValues(append(value.Labels, c.module)...).Set(value.Value)
			}

			now := time.Now()
			nextRun := sched.Next(now)
			select {
			case <-time.After(nextRun.Sub(now)):
			case <-c.closed:
				return
			}
		}
	}()
}

func (c *Collector) Close() error {
	close(c.closed)
	return nil
}
