package metadata

import (
	"go-dictionary/internal/db/postgres"
	"go-dictionary/internal/db/rocksdb"
	"go-dictionary/internal/messages"
)

type (
	MetadataClient struct {
		pgClient      metadataRepoClient
		rocksdbClient *rocksdb.RockClient
		tableNames    []string
	}

	metadataRepoClient struct {
		*postgres.PostgresClient
	}
)

func NewMetadataClient(
	pgClient *postgres.PostgresClient,
	rocksdbClient *rocksdb.RockClient,
) *MetadataClient {
	return &MetadataClient{
		pgClient: metadataRepoClient{
			pgClient,
		},
		rocksdbClient: rocksdbClient,
	}
}

func (client *MetadataClient) Run() {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		META_CLIENT_START,
	).ConsoleLog()

	client.tableNames = client.pgClient.getTablesName()
	tablesEstimates := client.getRowCountEstimates()
	client.pgClient.initTables(tablesEstimates)

	client.pgClient.setIndexerHealthy(true)
	go client.updateStatusLive()
}

// getRowCountEstimates calculates the list with the row counts estimates for each table
func (client *MetadataClient) getRowCountEstimates() []RowCountEstimate {
	tablesEstimates := make([]RowCountEstimate, len(client.tableNames))
	for idx, tableName := range client.tableNames {
		tableCount := client.pgClient.getTableRowsCount(tableName)
		tablesEstimates[idx].Estimate = tableCount
		tablesEstimates[idx].Table = tableName
	}
	return tablesEstimates
}

// updateStatusLive modifies the metadata rows live
func (client *MetadataClient) updateStatusLive() {
	defer client.pgClient.setIndexerHealthy(false)
	for {
		//TODO: update rows
	}
}
