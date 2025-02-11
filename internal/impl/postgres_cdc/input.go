package postgres_cdc

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lucasepe/codename"
	"github.com/usedatabrew/benthos/v4/public/service"
	"github.com/usedatabrew/pglogicalstream"
	"strings"
)

const statusHeartbeatIntervalSeconds = 10
const outputPlugin = "wal2json"

var randomSlotName string

var pgStreamConfigSpec = service.NewConfigSpec().
	Summary("Creates Postgres replication slot for CDC").
	Field(service.NewStringField("host").
		Description("PostgreSQL instance host").
		Example("123.0.0.1")).
	Field(service.NewIntField("port").
		Description("PostgreSQL instance port").
		Example(5432).
		Default(5432)).
	Field(service.NewStringField("user").
		Description("Username with permissions to start replication (RDS superuser)").
		Example("postgres"),
	).
	Field(service.NewStringField("password").
		Description("PostgreSQL database password")).
	Field(service.NewStringField("schema").
		Description("Schema that will be used to create replication")).
	Field(service.NewStringField("database").
		Description("PostgreSQL database name")).
	Field(service.NewBoolField("use_tls").
		Description("Defines whether benthos need to verify (skipinsecure) TLS configuration").
		Example(true).
		Default(false)).
	Field(service.NewBoolField("stream_snapshot").
		Description("Set `true` if you want to receive all the data that currently exist in database").
		Example(true).
		Default(false)).
	Field(service.NewFloatField("snapshot_memory_safety_factor").
		Description("Sets amount of memory that can be used to stream snapshot. If affects batch sizes. If we want to use only 25% of the memory available - put 0.25 factor. It will make initial streaming slower, but it will prevent your worker from OOM Kill").
		Example(0.2).
		Default(0.5)).
	Field(service.NewIntField("snapshot_batch_size").
		Description("Specifies number of messages in one batch while reading the snapshot. If set 0 - automatic batch size will be applied").
		Example(10_000).
		Default(10_000)).
	Field(service.NewObjectListField("plugin_schema",
		service.NewStringField("table"),
		service.NewObjectListField("columns",
			service.NewStringField("name").Description("Name of the column"),
			service.NewStringField("databrewType").Description("Apache Arrow type that will be used"),
			service.NewStringField("nativeConnectorType").Description("PostgreSQL column type"),
			service.NewBoolField("pk").Description("Specify the column as Primary Key"),
			service.NewBoolField("nullable").Description("Specify nullable field"),
		),
	)).
	Field(service.NewStringListField("tables").
		Example(`
			- my_table
			- my_table_2
		`).
		Description("List of tables we have to create logical replication for")).
	Field(service.NewStringField("slot_name").
		Description("PostgeSQL logical replication slot name. You can create it manually before starting the sync. If not provided will be replaced with a random one").
		Example("my_test_slot").
		Default(randomSlotName))

func newPgStreamInput(conf *service.ParsedConfig, logger *service.Logger) (s service.Input, err error) {
	var (
		dbName                  string
		dbPort                  int
		dbHost                  string
		dbSchema                string
		dbUser                  string
		dbPassword              string
		dbSlotName              string
		tables                  []string
		streamSnapshot          bool
		snapshotMemSafetyFactor float64
		snapshotBatchSize       int
	)

	dbSchema, err = conf.FieldString("schema")
	if err != nil {
		return nil, err
	}

	dbSlotName, err = conf.FieldString("slot_name")
	if err != nil {
		return nil, err
	}

	if dbSlotName == "" {
		dbSlotName = randomSlotName
	}

	dbPassword, err = conf.FieldString("password")
	if err != nil {
		return nil, err
	}

	dbUser, err = conf.FieldString("user")
	if err != nil {
		return nil, err
	}

	dbName, err = conf.FieldString("database")
	if err != nil {
		return nil, err
	}

	dbHost, err = conf.FieldString("host")
	if err != nil {
		return nil, err
	}

	dbPort, err = conf.FieldInt("port")
	if err != nil {
		return nil, err
	}

	tables, err = conf.FieldStringList("tables")
	if err != nil {
		return nil, err
	}

	streamSnapshot, err = conf.FieldBool("stream_snapshot")
	if err != nil {
		return nil, err
	}

	snapshotMemSafetyFactor, err = conf.FieldFloat("snapshot_memory_safety_factor")
	if err != nil {
		return nil, err
	}

	snapshotBatchSize, err = conf.FieldInt("snapshot_batch_size")
	if err != nil {
		return nil, err
	}

	var schemaConfig []*service.ParsedConfig
	schemaConfig, err = conf.FieldObjectList("plugin_schema")
	if err != nil {
		return nil, err
	}
	dbTableSchemas := buildDataSchemas(schemaConfig)

	return service.AutoRetryNacks(&pgStreamInput{
		dbConfig: pgconn.Config{
			Host:     dbHost,
			Port:     uint16(dbPort),
			Database: dbName,
			User:     dbUser,
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			Password: dbPassword,
		},
		streamSnapshot:          streamSnapshot,
		snapshotMemSafetyFactor: snapshotMemSafetyFactor,
		snapshotBatchSize:       snapshotBatchSize,
		slotName:                dbSlotName,
		tablesSchema:            dbTableSchemas,
		schema:                  dbSchema,
		tables:                  tables,
		logger:                  logger,
	}), err
}

func init() {
	rng, _ := codename.DefaultRNG()
	randomSlotName = fmt.Sprintf("rs_%s", strings.ReplaceAll(codename.Generate(rng, 5), "-", "_"))

	err := service.RegisterInput(
		"pg_stream", pgStreamConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newPgStreamInput(conf, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

type pgStreamInput struct {
	dbConfig                pgconn.Config
	pglogicalStream         *pglogicalstream.Stream
	slotName                string
	schema                  string
	tables                  []string
	tablesSchema            []pglogicalstream.DbTablesSchema
	streamSnapshot          bool
	snapshotMemSafetyFactor float64
	snapshotBatchSize       int
	logger                  *service.Logger
}

func (p *pgStreamInput) Connect(ctx context.Context) error {
	pgStream, err := pglogicalstream.NewPgStream(pglogicalstream.Config{
		DbHost:                     p.dbConfig.Host,
		DbPassword:                 p.dbConfig.Password,
		DbUser:                     p.dbConfig.User,
		DbPort:                     int(p.dbConfig.Port),
		DbName:                     p.dbConfig.Database,
		DbSchema:                   p.schema,
		DbTablesSchema:             p.tablesSchema,
		ReplicationSlotName:        fmt.Sprintf("rs_%s", p.slotName),
		TlsVerify:                  "require",
		StreamOldData:              p.streamSnapshot,
		SnapshotMemorySafetyFactor: p.snapshotMemSafetyFactor,
		BatchSize:                  p.snapshotBatchSize,
		SeparateChanges:            true,
	})
	if err != nil {
		panic(err)
	}

	p.pglogicalStream = pgStream

	return err
}

func (p *pgStreamInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case snapshotMessage := <-p.pglogicalStream.SnapshotMessageC():
		// messages are produced one by one.
		// therefore we can assume that 0 index always contains the table with changes
		snapshotMessageEncoded, _ := json.Marshal(&snapshotMessage.Changes[0].Row)
		var m []interface{}
		err := json.Unmarshal(snapshotMessageEncoded, &m)
		if err != nil {
			return nil, nil, err
		}
		snapshotMessageEncoded, _ = json.Marshal(&m[0])

		createdMessage := service.NewMessage(snapshotMessageEncoded)
		// snapshot messages are produced one by one.
		// therefore we can assume that 0 index always contains the table with changes
		createdMessage.MetaSet("table", snapshotMessage.Changes[0].Table)
		createdMessage.MetaSet("snapshot", "true")
		createdMessage.MetaSet("schema", snapshotMessage.Changes[0].Schema)
		createdMessage.MetaSet("event", snapshotMessage.Changes[0].Kind)
		return createdMessage, func(ctx context.Context, err error) error {
			// Nacks are retried automatically when we use service.AutoRetryNacks
			//message.ServerHeartbeat.

			//p.lrAckLSN(lsn)
			return nil
		}, nil
	case message := <-p.pglogicalStream.LrMessageC():
		// messages are produced one by one.
		// therefore we can assume that 0 index always contains the table with changes
		messageEncoded, _ := json.Marshal(&message.Changes[0].Row)
		var m []interface{}
		err := json.Unmarshal(messageEncoded, &m)
		if err != nil {
			return nil, nil, err
		}
		messageEncoded, _ = json.Marshal(&m[0])
		createdMessage := service.NewMessage(messageEncoded)
		createdMessage.MetaSet("table", message.Changes[0].Table)
		createdMessage.MetaSet("schema", message.Changes[0].Schema)
		createdMessage.MetaSet("event", message.Changes[0].Kind)
		return createdMessage, func(ctx context.Context, err error) error {
			p.logger.Infof("ack lsn %s", message.Lsn)
			p.pglogicalStream.AckLSN(message.Lsn)
			return nil
		}, nil
	case <-ctx.Done():

	}

	return nil, nil, errors.New("action timed out")
}

func (p *pgStreamInput) Close(ctx context.Context) error {
	if p.pglogicalStream != nil {
		return p.pglogicalStream.Stop()
	}
	return nil
}
