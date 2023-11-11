package postgres_cdc

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/usedatabrew/benthos/v4/public/service"
	"github.com/usedatabrew/pglogicalstream"
)

type DataSchema struct {
	TableName string
	Schema    *arrow.Schema
}

func buildDataSchemas(config []*service.ParsedConfig) (schemas []pglogicalstream.DbTablesSchema) {
	for _, tableConfig := range config {
		name, err := tableConfig.FieldString("table")
		if err != nil {
			panic("Can't build schema")
		}

		var dbTableSchema pglogicalstream.DbTablesSchema
		dbTableSchema.Table = name
		var columnsConfig []*service.ParsedConfig
		if columnsConfig, err = tableConfig.FieldObjectList("columns"); err != nil {
			panic("Can't build schema")
		}

		for _, columnConfig := range columnsConfig {
			columnType, _ := columnConfig.FieldString("databrewType")
			nativeColumnType, _ := columnConfig.FieldString("nativeConnectorType")
			columnName, _ := columnConfig.FieldString("name")
			columnIsIdentity, _ := columnConfig.FieldBool("nullable")
			columnNullable, _ := columnConfig.FieldBool("pk")

			column := pglogicalstream.DbSchemaColumn{
				Name:                columnName,
				DatabrewType:        columnType,
				NativeConnectorType: nativeColumnType,
				Pk:                  columnIsIdentity,
				Nullable:            columnNullable,
			}
			dbTableSchema.Columns = append(dbTableSchema.Columns, column)
		}

		schemas = append(schemas, dbTableSchema)
	}

	return
}
