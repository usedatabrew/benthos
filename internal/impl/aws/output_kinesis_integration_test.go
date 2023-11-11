package aws

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/usedatabrew/benthos/v4/internal/integration"
	"github.com/usedatabrew/benthos/v4/public/service"
)

func TestKinesisIntegration(t *testing.T) {
	integration.CheckSkip(t)

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	// start mysql container with binlog enabled
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "vsouza/kinesis-local",
		Cmd: []string{
			"--createStreamMs=5",
		},
	})
	if err != nil {
		t.Fatalf("Could not start resource: %v", err)
	}
	defer func() {
		if err := pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	port, err := strconv.ParseInt(resource.GetPort("4567/tcp"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("http://localhost:%d", port)

	pConf, err := koOutputSpec().ParseYAML(fmt.Sprintf(`
stream: foo
partition_key: ${! json("id") }
region: us-east-1
endpoint: "%v"
credentials:
  id: xxxxxx
  secret: xxxxxx
  token: xxxxxx
`, endpoint), nil)
	require.NoError(t, err)

	config, err := koConfigFromParsed(pConf)
	require.NoError(t, err)

	// bootstrap kinesis
	client := kinesis.New(session.Must(session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		Endpoint:    aws.String(endpoint),
		Region:      aws.String("us-east-1"),
	})))
	if err := pool.Retry(func() error {
		_, err := client.CreateStream(&kinesis.CreateStreamInput{
			ShardCount: aws.Int64(1),
			StreamName: aws.String("foo"),
		})
		return err
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	t.Run("testKinesisConnect", func(t *testing.T) {
		testKinesisConnect(t, config, client)
	})

	t.Run("testKinesisConnectWithInvalidStream", func(t *testing.T) {
		config.Stream = "invalid-foo"
		testKinesisConnectWithInvalidStream(t, config, client)
	})
}

func testKinesisConnect(t *testing.T, c koConfig, client *kinesis.Kinesis) {
	r, err := newKinesisWriter(c, service.MockResources())
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		require.NoError(t, r.Close(context.Background()))
	}()

	records := [][]byte{
		[]byte(`{"foo":"bar","id":123}`),
		[]byte(`{"foo":"baz","id":456}`),
		[]byte(`{"foo":"qux","id":789}`),
	}

	var msg service.MessageBatch
	for _, record := range records {
		msg = append(msg, service.NewMessage(record))
	}

	if err := r.WriteBatch(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	iterator, err := client.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:           aws.String("shardId-000000000000"),
		ShardIteratorType: aws.String(kinesis.ShardIteratorTypeTrimHorizon),
		StreamName:        aws.String(c.Stream),
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.GetRecords(&kinesis.GetRecordsInput{
		Limit:         aws.Int64(10),
		ShardIterator: iterator.ShardIterator,
	})
	if err != nil {
		t.Error(err)
	}
	if act, exp := len(out.Records), len(records); act != exp {
		t.Fatalf("Expected GetRecords response to have records with length of %d, got %d", exp, act)
	}
	for i, record := range records {
		if !bytes.Equal(out.Records[i].Data, record) {
			t.Errorf("Expected record %d to equal %v, got %v", i, record, out.Records[i])
		}
	}
}

func testKinesisConnectWithInvalidStream(t *testing.T, c koConfig, client *kinesis.Kinesis) {
	r, err := newKinesisWriter(c, service.MockResources())
	if err != nil {
		t.Fatal(err)
	}

	retries := 3
	for i := 0; i < retries; i++ {
		err := r.Connect(context.Background())
		assert.Error(t, err)
	}
}
