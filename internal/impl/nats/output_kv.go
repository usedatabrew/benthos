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

func natsKVOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.12.0").
		Summary("Put messages in a NATS key-value bucket.").
		Description(`
The field ` + "`key`" + ` supports
[interpolation functions](/docs/configuration/interpolation#bloblang-queries), allowing
you to create a unique key for each message.

` + ConnectionNameDescription() + auth.Description()).
		Field(service.NewStringListField("urls").
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"nats://127.0.0.1:4222"}).
			Example([]string{"nats://username:password@127.0.0.1:4222"})).
		Field(service.NewStringField("bucket").
			Description("The name of the KV bucket to operate on.").
			Example("my_kv_bucket")).
		Field(service.NewInterpolatedStringField("key").
			Description("The key for each message.").
			Example("foo").
			Example("foo.bar.baz").
			Example(`foo.${! json("meta.type") }`)).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").
			Default(1024)).
		Field(service.NewTLSToggledField("tls")).
		Field(service.NewInternalField(auth.FieldSpec()))
}

func init() {
	err := service.RegisterOutput(
		"nats_kv", natsKVOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			maxInFlight, err := conf.FieldInt("max_in_flight")
			if err != nil {
				return nil, 0, err
			}
			w, err := newKVOutput(conf, mgr)
			return w, maxInFlight, err
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type kvOutput struct {
	label  string
	urls   string
	bucket string
	key    *service.InterpolatedString
	keyRaw string

	authConf auth.Config
	tlsConf  *tls.Config

	log *service.Logger
	fs  *service.FS

	connMut  sync.Mutex
	natsConn *nats.Conn
	keyValue nats.KeyValue

	shutSig *shutdown.Signaller
}

func newKVOutput(conf *service.ParsedConfig, mgr *service.Resources) (*kvOutput, error) {
	kv := kvOutput{
		label:   mgr.Label(),
		log:     mgr.Logger(),
		fs:      mgr.FS(),
		shutSig: shutdown.NewSignaller(),
	}

	urlList, err := conf.FieldStringList("urls")
	if err != nil {
		return nil, err
	}
	kv.urls = strings.Join(urlList, ",")

	if kv.bucket, err = conf.FieldString("bucket"); err != nil {
		return nil, err
	}

	if kv.keyRaw, err = conf.FieldString("key"); err != nil {
		return nil, err
	}

	if kv.key, err = conf.FieldInterpolatedString("key"); err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		kv.tlsConf = tlsConf
	}

	if kv.authConf, err = AuthFromParsedConfig(conf.Namespace("auth")); err != nil {
		return nil, err
	}
	return &kv, nil
}

//------------------------------------------------------------------------------

func (kv *kvOutput) Connect(ctx context.Context) error {
	kv.connMut.Lock()
	defer kv.connMut.Unlock()

	if kv.natsConn != nil {
		return nil
	}

	var natsConn *nats.Conn
	var err error

	defer func() {
		if err != nil && natsConn != nil {
			natsConn.Close()
		}
	}()

	var opts []nats.Option
	if kv.tlsConf != nil {
		opts = append(opts, nats.Secure(kv.tlsConf))
	}
	opts = append(opts, nats.Name(kv.label))
	opts = append(opts, authConfToOptions(kv.authConf, kv.fs)...)
	if natsConn, err = nats.Connect(kv.urls, opts...); err != nil {
		return err
	}

	jsc, err := natsConn.JetStream()
	if err != nil {
		return err
	}

	kv.keyValue, err = jsc.KeyValue(kv.bucket)
	if err != nil {
		return err
	}

	kv.log.Infof("Setting values on NATS KV bucket: %s and key: %s", kv.bucket, kv.keyRaw)

	kv.natsConn = natsConn
	return nil
}

func (kv *kvOutput) disconnect() {
	kv.connMut.Lock()
	defer kv.connMut.Unlock()

	if kv.natsConn != nil {
		kv.natsConn.Close()
		kv.natsConn = nil
	}
	kv.keyValue = nil
}

//------------------------------------------------------------------------------

func (kv *kvOutput) Write(ctx context.Context, msg *service.Message) error {
	kv.connMut.Lock()
	keyValue := kv.keyValue
	kv.connMut.Unlock()
	if keyValue == nil {
		return service.ErrNotConnected
	}

	value, err := msg.AsBytes()
	if err != nil {
		return err
	}

	key, err := kv.key.TryString(msg)
	if err != nil {
		return err
	}

	rev, err := keyValue.Put(key, value)
	if err != nil {
		return err
	}

	kv.log.With(
		metaKVBucket, keyValue.Bucket(),
		metaKVKey, key,
		metaKVRevision, rev,
	).Debug("Updated kv bucket entry")

	return nil
}

func (kv *kvOutput) Close(ctx context.Context) error {
	go func() {
		kv.disconnect()
		kv.shutSig.ShutdownComplete()
	}()
	select {
	case <-kv.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
