package influxdb

import (
	"fmt"
    "context"

	client "github.com/influxdata/influxdb-client-go/v2"
	"github.com/jlehtimaki/drone-exporter/pkg/env"
	"github.com/jlehtimaki/drone-exporter/pkg/types"
)

var (
	influxAddress = env.GetEnv("INFLUXDB_ADDRESS", "http://influxdb:8086")
	bucket        = env.GetEnv("INFLUXDB_BUCKET", "example")
	token         = env.GetEnv("INFLUXDB_TOKEN", "token")
	org         = env.GetEnv("INFLUXDB_ORG", "org")
)

const LastBuildIdQueryFmt = `from(bucket: "%s")
                               |> range(start: -30d, stop: 0s)
                               |> filter(fn: (r) => r["_measurement"] == "builds")
                               |> filter(fn: (r) => r["Slug"] == "%s")
                               |> drop(columns: ["Status"])
                               |> filter(fn: (r) => r["_field"] == "BuildId")
                                 |> max()`

type driver struct {
	client client.Client
}

func NewDriver() (*driver, error) {
	client, err := getClient()
	if err != nil {
		return nil, err
	}
	return &driver{
		client: client,
	}, nil
}

func getClient() (client.Client, error) {
	c := client.NewClient(influxAddress, token)

    // validate client connection health
    _, err := c.Health(context.Background())

	if err != nil {
		return nil, err
	}

	return c, nil
}

func (d *driver) Close() error {
	d.client.Close()
	return nil
}

func (d *driver) LastBuildNumber(slug string) int64 {
    queryAPI := d.client.QueryAPI(org)
    //     get QueryTableResult
    result, err := queryAPI.Query(context.Background(), fmt.Sprintf(LastBuildIdQueryFmt, bucket, slug))

    if err != nil {
        panic(err)
        return 0
    }

    result.Next()
    if result.Err() != nil {
        fmt.Printf("query parsing error: %s\n", result.Err().Error())
    }

    if result.Record() == nil {
        return 0
    }

    ret := result.Record().Value()

    return ret.(int64)
}

func (d *driver) Batch(points []types.Point) error {
    // Get non-blocking write client
    writeAPI := d.client.WriteAPI(org, bucket)
    // Get errors channel
    errorsCh := writeAPI.Errors()
    // Create go proc for reading and logging errors
    go func() {
        for err := range errorsCh {
            fmt.Printf("write error: %s\n", err.Error())
        }
    }()

	i := 0
	for _, point := range points {

		pt := client.NewPoint(point.GetMeasurement(), point.GetTags(), point.GetFields(), point.GetTime())

        writeAPI.WritePoint(pt)
		i++

	}

	// Force all unwritten data to be sent
    writeAPI.Flush()

    // Ensures background processes finishes
    d.client.Close()
	return nil
}
