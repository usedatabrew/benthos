package nats

import (
	"context"
	"crypto/tls"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/usedatabrew/benthos/v4/internal/impl/nats/auth"
	"github.com/usedatabrew/benthos/v4/internal/shutdown"
	"github.com/usedatabrew/benthos/v4/public/service"
)

func natsKVInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.12.0").
		Summary("Watches for updates in a NATS key-value bucket.").
		Description(`
### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- nats_kv_key
- nats_kv_bucket
- nats_kv_revision
- nats_kv_delta
- nats_kv_operation
- nats_kv_created
` + "```" + `

` + ConnectionNameDescription() + auth.Description()).
		Field(service.NewStringListField("urls").
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"nats://127.0.0.1:4222"}).
			Example([]string{"nats://username:password@127.0.0.1:4222"})).
		Field(service.NewStringField("bucket").
			Description("The name of the KV bucket to watch for updates.").
			Example("my_kv_bucket")).
		Field(service.NewStringField("key").
			Description("Key to watch for updates, can include wildcards.").
			Default(">").
			Example("foo.bar.baz").Example("foo.*.baz").Example("foo.bar.*").Example("foo.>")).
		Field(service.NewBoolField("ignore_deletes").
			Description("Do not send delete markers as messages.").
			Default(false).
			Advanced()).
		Field(service.NewBoolField("include_history").
			Description("Include all the history per key, not just the last one.").
			Default(false).
			Advanced()).
		Field(service.NewBoolField("meta_only").
			Description("Retrieve only the metadata of the entry").
			Default(false).
			Advanced()).
		Field(service.NewTLSToggledField("tls")).
		Field(service.NewInternalField(auth.FieldSpec()))
}

func init() {
	err := service.RegisterInput(
		"nats_kv", natsKVInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			reader, err := newKVReader(conf, mgr)
			return service.AutoRetryNacks(reader), err
		},
	)
	if err != nil {
		panic(err)
	}
}

type kvReader struct {
	label          string
	urls           string
	bucket         string
	key            string
	ignoreDeletes  bool
	includeHistory bool
	metaOnly       bool
	authConf       auth.Config
	tlsConf        *tls.Config

	log *service.Logger
	fs  *service.FS

	shutSig *shutdown.Signaller

	connMut  sync.Mutex
	natsConn *nats.Conn
	watcher  nats.KeyWatcher
}

func newKVReader(conf *service.ParsedConfig, mgr *service.Resources) (*kvReader, error) {
	r := &kvReader{
		label:   mgr.Label(),
		log:     mgr.Logger(),
		fs:      mgr.FS(),
		shutSig: shutdown.NewSignaller(),
	}

	urlList, err := conf.FieldStringList("urls")
	if err != nil {
		return nil, err
	}
	r.urls = strings.Join(urlList, ",")

	if r.bucket, err = conf.FieldString("bucket"); err != nil {
		return nil, err
	}

	if r.ignoreDeletes, err = conf.FieldBool("ignore_deletes"); err != nil {
		return nil, err
	}

	if r.includeHistory, err = conf.FieldBool("include_history"); err != nil {
		return nil, err
	}

	if r.metaOnly, err = conf.FieldBool("meta_only"); err != nil {
		return nil, err
	}

	if r.key, err = conf.FieldString("key"); err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		r.tlsConf = tlsConf
	}

	if r.authConf, err = AuthFromParsedConfig(conf.Namespace("auth")); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *kvReader) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	if r.natsConn != nil {
		return nil
	}

	var err error

	defer func() {
		if err != nil {
			if r.watcher != nil {
				_ = r.watcher.Stop()
			}
			if r.natsConn != nil {
				r.natsConn.Close()
			}
		}
	}()

	var opts []nats.Option
	if r.tlsConf != nil {
		opts = append(opts, nats.Secure(r.tlsConf))
	}
	opts = append(opts, nats.Name(r.label))
	opts = append(opts, authConfToOptions(r.authConf, r.fs)...)
	if r.natsConn, err = nats.Connect(r.urls, opts...); err != nil {
		return err
	}

	js, err := r.natsConn.JetStream()
	if err != nil {
		return err
	}

	kv, err := js.KeyValue(r.bucket)
	if err != nil {
		return err
	}

	var watchOpts []nats.WatchOpt
	if r.ignoreDeletes {
		watchOpts = append(watchOpts, nats.IgnoreDeletes())
	}
	if r.includeHistory {
		watchOpts = append(watchOpts, nats.IncludeHistory())
	}
	if r.metaOnly {
		watchOpts = append(watchOpts, nats.MetaOnly())
	}

	r.watcher, err = kv.Watch(r.key, watchOpts...)
	if err != nil {
		return err
	}

	r.log.Infof("Watching NATS KV bucket: %s for key(s): %s", r.bucket, r.key)

	return nil
}

func (r *kvReader) disconnect() {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	if r.watcher != nil {
		_ = r.watcher.Stop()
		r.watcher = nil
	}
	if r.natsConn != nil {
		r.natsConn.Close()
		r.natsConn = nil
	}
}

func (r *kvReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	r.connMut.Lock()
	watcher := r.watcher
	r.connMut.Unlock()

	if watcher == nil {
		return nil, nil, service.ErrNotConnected
	}

	for {
		var entry nats.KeyValueEntry
		var open bool
		select {
		case entry, open = <-watcher.Updates():
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}

		if !open {
			r.disconnect()
			return nil, nil, service.ErrNotConnected
		}

		if entry == nil {
			continue
		}

		r.log.With(
			metaKVBucket, entry.Bucket(),
			metaKVKey, entry.Key(),
			metaKVRevision, entry.Revision(),
			metaKVOperation, entry.Operation().String(),
		).Debugf("Received kv bucket update")

		return newMessageFromKVEntry(entry), func(ctx context.Context, res error) error {
			return nil
		}, nil
	}
}

func (r *kvReader) Close(ctx context.Context) error {
	go func() {
		r.disconnect()
		r.shutSig.ShutdownComplete()
	}()
	select {
	case <-r.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
