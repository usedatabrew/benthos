package nats

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/nats-io/nats.go"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/usedatabrew/benthos/v4/internal/integration"
	"github.com/usedatabrew/benthos/v4/public/service"
)

func TestIntegrationNatsKV(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "nats",
		Tag:        "latest",
		Cmd:        []string{"--js", "--trace"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var natsConn *nats.Conn
	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		natsConn, err = nats.Connect(fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp")))
		return err
	}))
	t.Cleanup(func() {
		natsConn.Close()
	})

	template := `
output:
  label: kv_output
  nats_kv:
    urls: [ tcp://localhost:$PORT ]
    bucket: bucket-$ID
    # We need to make this key random as the NATS server will only deliver the
    # latest revision of a key when it's requested by a watcher, this is by
    # design, but if we want to test benthos semantics like batching we should
    # use unique keys for every message passing through the output
    key: ${! ksuid() }

input:
  label: kv_input
  nats_kv:
    urls: [ tcp://localhost:$PORT ]
    bucket: bucket-$ID
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		// integration.StreamTestMetadata(), // NATS KV doesn't support metadata
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallelLossy(1000),
		integration.StreamTestStreamParallelLossyThroughReconnect(1000),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, _ context.Context, testID string, _ *integration.StreamTestConfigVars) {
			js, err := natsConn.JetStream()
			require.NoError(t, err)

			bucketName := "bucket-" + testID

			_, err = js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket: bucketName,
			})
			require.NoError(t, err)
		}),
		integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
		integration.StreamTestOptPort(resource.GetPort("4222/tcp")),
	)

	t.Run("processor", func(t *testing.T) {
		createBucket := func(t *testing.T) (nats.KeyValue, string) {
			u4, err := uuid.NewV4()
			require.NoError(t, err)
			js, err := natsConn.JetStream()
			require.NoError(t, err)

			bucketName := "bucket-" + u4.String()

			bucket, err := js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:  bucketName,
				History: 5,
			})
			require.NoError(t, err)

			url := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp"))

			return bucket, url
		}

		process := func(yaml string) (service.MessageBatch, error) {
			spec := natsKVProcessorConfig()
			parsed, err := spec.ParseYAML(yaml, nil)
			require.NoError(t, err)

			p, err := newKVProcessor(parsed, service.MockResources())
			require.NoError(t, err)

			m := service.NewMessage([]byte("hello"))
			return p.Process(context.Background(), m)
		}

		t.Run("get operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			_, err := bucket.PutString("blob", "lawblog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: get
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("lawblog"), bytes)
		})

		t.Run("get_revision operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			revision, err := bucket.PutString("blob", "lawblog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: get_revision
        key: blob
        revision: %d
        urls: [%s]`, bucket.Bucket(), revision, url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("lawblog"), bytes)
		})

		t.Run("create operation (success)", func(t *testing.T) {
			bucket, url := createBucket(t)
			yaml := fmt.Sprintf(`
        bucket: %s
        operation: create
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("hello"), bytes)
		})

		t.Run("create operation (error)", func(t *testing.T) {
			bucket, url := createBucket(t)
			_, err := bucket.PutString("blob", "lawblog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: create
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			_, err = process(yaml)
			require.Error(t, err)
		})

		t.Run("put operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			yaml := fmt.Sprintf(`
        bucket: %s
        operation: put
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("hello"), bytes)
		})

		t.Run("update operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			revision, err := bucket.PutString("blob", "lawblog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: update
        key: blob
        revision: %d
        urls: [%s]`, bucket.Bucket(), revision, url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("hello"), bytes)
		})

		t.Run("delete operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			_, err := bucket.PutString("blob", "lawblog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: delete
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("hello"), bytes)

			_, err = bucket.Get("blob")
			require.Error(t, err)
		})

		t.Run("purge operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			_, err := bucket.PutString("blob", "lawblog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: purge
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("hello"), bytes)

			_, err = bucket.Get("blob")
			require.Error(t, err)
		})

		t.Run("history operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			_, err := bucket.PutString("blob", "lawblog")
			require.NoError(t, err)
			_, err = bucket.PutString("blob", "sawedlog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: history
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			result, err := process(yaml)
			require.NoError(t, err)

			assert.Equal(t, 2, len(result))
		})

		t.Run("keys operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			_, err := bucket.PutString("blob", "lawblog")
			require.NoError(t, err)
			_, err = bucket.PutString("bobs", "sawedlog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: keys
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			result, err := process(yaml)
			require.NoError(t, err)

			assert.Equal(t, 2, len(result))
		})
	})
}
