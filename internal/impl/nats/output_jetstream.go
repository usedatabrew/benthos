package nats

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/usedatabrew/benthos/v4/internal/component/output/span"
	"github.com/usedatabrew/benthos/v4/internal/impl/nats/auth"
	"github.com/usedatabrew/benthos/v4/internal/shutdown"
	"github.com/usedatabrew/benthos/v4/public/service"
)

func natsJetStreamOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Version("3.46.0").
		Summary("Write messages to a NATS JetStream subject.").
		Description(ConnectionNameDescription() + auth.Description()).
		Field(service.NewStringListField("urls").
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"nats://127.0.0.1:4222"}).
			Example([]string{"nats://username:password@127.0.0.1:4222"})).
		Field(service.NewInterpolatedStringField("subject").
			Description("A subject to write to.").
			Example("foo.bar.baz").
			Example(`${! meta("kafka_topic") }`).
			Example(`foo.${! json("meta.type") }`)).
		Field(service.NewInterpolatedStringMapField("headers").
			Description("Explicit message headers to add to messages.").
			Default(map[string]any{}).
			Example(map[string]any{
				"Content-Type": "application/json",
				"Timestamp":    `${!meta("Timestamp")}`,
			}).Version("4.1.0")).
		Field(service.NewMetadataFilterField("metadata").
			Description("Determine which (if any) metadata values should be added to messages as headers.").
			Optional()).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").
			Default(1024)).
		Field(service.NewTLSToggledField("tls")).
		Field(service.NewInternalField(auth.FieldSpec())).
		Field(span.InjectTracingSpanMappingDocs().Version(tracingVersion))
}

func init() {
	err := service.RegisterOutput(
		"nats_jetstream", natsJetStreamOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			maxInFlight, err := conf.FieldInt("max_in_flight")
			if err != nil {
				return nil, 0, err
			}
			w, err := newJetStreamWriterFromConfig(conf, mgr)
			if err != nil {
				return nil, 0, err
			}
			spanOutput, err := span.NewOutput("nats_jetstream", conf, w, mgr)
			return spanOutput, maxInFlight, err
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type jetStreamOutput struct {
	label         string
	urls          string
	subjectStrRaw string
	subjectStr    *service.InterpolatedString
	headers       map[string]*service.InterpolatedString
	metaFilter    *service.MetadataFilter
	authConf      auth.Config
	tlsConf       *tls.Config

	log *service.Logger
	fs  *service.FS

	connMut  sync.Mutex
	natsConn *nats.Conn
	jCtx     nats.JetStreamContext

	shutSig *shutdown.Signaller
}

func newJetStreamWriterFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*jetStreamOutput, error) {
	j := jetStreamOutput{
		label:   mgr.Label(),
		log:     mgr.Logger(),
		fs:      mgr.FS(),
		shutSig: shutdown.NewSignaller(),
	}

	urlList, err := conf.FieldStringList("urls")
	if err != nil {
		return nil, err
	}
	j.urls = strings.Join(urlList, ",")

	if j.subjectStrRaw, err = conf.FieldString("subject"); err != nil {
		return nil, err
	}

	if j.subjectStr, err = conf.FieldInterpolatedString("subject"); err != nil {
		return nil, err
	}

	if j.headers, err = conf.FieldInterpolatedStringMap("headers"); err != nil {
		return nil, err
	}

	if conf.Contains("metadata") {
		if j.metaFilter, err = conf.FieldMetadataFilter("metadata"); err != nil {
			return nil, err
		}
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		j.tlsConf = tlsConf
	}

	if j.authConf, err = AuthFromParsedConfig(conf.Namespace("auth")); err != nil {
		return nil, err
	}
	return &j, nil
}

//------------------------------------------------------------------------------

func (j *jetStreamOutput) Connect(ctx context.Context) error {
	j.connMut.Lock()
	defer j.connMut.Unlock()

	if j.natsConn != nil {
		return nil
	}

	var natsConn *nats.Conn
	var jCtx nats.JetStreamContext
	var err error

	defer func() {
		if err != nil && natsConn != nil {
			natsConn.Close()
		}
	}()

	var opts []nats.Option
	if j.tlsConf != nil {
		opts = append(opts, nats.Secure(j.tlsConf))
	}
	opts = append(opts, nats.Name(j.label))
	opts = append(opts, authConfToOptions(j.authConf, j.fs)...)
	opts = append(opts, errorHandlerOption(j.log))
	if natsConn, err = nats.Connect(j.urls, opts...); err != nil {
		return err
	}

	if jCtx, err = natsConn.JetStream(); err != nil {
		return err
	}

	j.log.Infof("Sending NATS messages to JetStream subject: %v", j.subjectStrRaw)

	j.natsConn = natsConn
	j.jCtx = jCtx
	return nil
}

func (j *jetStreamOutput) disconnect() {
	j.connMut.Lock()
	defer j.connMut.Unlock()

	if j.natsConn != nil {
		j.natsConn.Close()
		j.natsConn = nil
	}
	j.jCtx = nil
}

//------------------------------------------------------------------------------

func (j *jetStreamOutput) Write(ctx context.Context, msg *service.Message) error {
	j.connMut.Lock()
	jCtx := j.jCtx
	j.connMut.Unlock()
	if jCtx == nil {
		return service.ErrNotConnected
	}

	subject, err := j.subjectStr.TryString(msg)
	if err != nil {
		return fmt.Errorf(`failed string interpolation on field "subject": %w`, err)
	}

	jsmsg := nats.NewMsg(subject)
	msgBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}

	jsmsg.Data = msgBytes
	for k, v := range j.headers {
		value, err := v.TryString(msg)
		if err != nil {
			return fmt.Errorf(`failed string interpolation on header %q: %w`, k, err)
		}

		jsmsg.Header.Add(k, value)
	}
	_ = j.metaFilter.Walk(msg, func(key, value string) error {
		jsmsg.Header.Add(key, value)
		return nil
	})

	_, err = jCtx.PublishMsg(jsmsg)
	return err
}

func (j *jetStreamOutput) Close(ctx context.Context) error {
	go func() {
		j.disconnect()
		j.shutSig.ShutdownComplete()
	}()
	select {
	case <-j.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
