package pure_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/usedatabrew/benthos/v4/internal/component/processor"
	"github.com/usedatabrew/benthos/v4/internal/manager/mock"
	"github.com/usedatabrew/benthos/v4/internal/message"
)

func TestDedupe(t *testing.T) {
	doc1 := []byte("hello world")
	doc2 := []byte("hello world")
	doc3 := []byte("hello world 2")

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	conf := processor.NewConfig()
	conf.Type = "dedupe"
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.Key = "${! content() }"

	proc, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	msgIn := message.QuickBatch([][]byte{doc1})
	msgOut, err := proc.ProcessBatch(context.Background(), msgIn)
	require.NoError(t, err)
	require.Len(t, msgOut, 1)

	msgIn = message.QuickBatch([][]byte{doc2})
	msgOut, err = proc.ProcessBatch(context.Background(), msgIn)
	require.NoError(t, err)
	require.Len(t, msgOut, 0)

	msgIn = message.QuickBatch([][]byte{doc3})
	msgOut, err = proc.ProcessBatch(context.Background(), msgIn)
	require.NoError(t, err)
	require.Len(t, msgOut, 1)

	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	proc, err = mgr.NewProcessor(conf)
	require.NoError(t, err)

	msgIn = message.QuickBatch([][]byte{doc1, doc2, doc3})
	msgOut, err = proc.ProcessBatch(context.Background(), msgIn)
	require.NoError(t, err)
	require.Len(t, msgOut, 1)
	assert.Equal(t, 2, msgOut[0].Len())
}

func TestDedupeBadCache(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "dedupe"
	conf.Dedupe.Cache = "foocache"

	mgr := mock.NewManager()
	_, err := mgr.NewProcessor(conf)
	require.Error(t, err)
}

func TestDedupeCacheErrors(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "dedupe"
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.Key = "${! content() }"

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	proc, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	delete(mgr.Caches, "foocache")

	msgs, err := proc.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("foo"), []byte("bar")}))
	require.NoError(t, err)
	assert.Len(t, msgs, 0)

	conf.Dedupe.DropOnCacheErr = false
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	proc, err = mgr.NewProcessor(conf)
	require.NoError(t, err)

	delete(mgr.Caches, "foocache")

	msgs, err = proc.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("foo"), []byte("bar")}))
	require.NoError(t, err)
	assert.Len(t, msgs, 1)
}
