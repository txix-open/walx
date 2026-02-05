package metric_test

import (
	"testing"
	"time"

	"github.com/txix-open/walx/v2/metric"
)

func Test(t *testing.T) {
	t.Parallel()

	metric.NewCollector(metric.EverySecond, "test").Add(metric.Metric{
		Name:        "test",
		Description: "some test metric",
		Labels:      []string{"voting_id"},
		Collect: func() []metric.Value {
			return []metric.Value{metric.ValueOf(3, "votingId")}
		},
	})
	time.Sleep(3 * time.Second)
}
