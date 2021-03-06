package metadata

import (
	"bytes"
	"encoding/json"
	"go-dictionary/internal/db/postgres"
	"go-dictionary/internal/db/rocksdb"
	"go-dictionary/internal/messages"
	"time"

	retryablehttp "github.com/hashicorp/go-retryablehttp"

	"github.com/itering/substrate-api-rpc/rpc"
)

type (
	MetadataClient struct {
		pgClient      metadataRepoClient
		rocksdbClient *rocksdb.RockClient
		httpEndpoint  string
		tableNames    []string
	}

	metadataRepoClient struct {
		*postgres.PostgresClient
		schemaName string
	}
)

var (
	CHAIN_MESSAGE = []byte(`{"id":1,"method":"system_chain","params":[],"jsonrpc":"2.0"}`)
)

func NewMetadataClient(
	pgClient *postgres.PostgresClient,
	rocksdbClient *rocksdb.RockClient,
	httpEndpoint string,
	schemaName string,
) *MetadataClient {
	return &MetadataClient{
		pgClient: metadataRepoClient{
			pgClient,
			schemaName,
		},
		rocksdbClient: rocksdbClient,
		httpEndpoint:  httpEndpoint,
	}
}

func (client *MetadataClient) Run(specName string) {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		META_CLIENT_START,
	).ConsoleLog()

	client.tableNames = client.pgClient.getTablesName()

	tablesEstimates := client.getRowCountEstimates()
	genesisHash := client.rocksdbClient.GetGenesisHash()
	chainName := client.getChainName()

	client.pgClient.initTables(
		tablesEstimates,
		"0x"+genesisHash,
		chainName,
		specName,
	)
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

func (client *MetadataClient) getChainName() string {
	reqBody := bytes.NewBuffer(CHAIN_MESSAGE)
	retryClient := retryablehttp.NewClient()
	retryClient.RetryWaitMin = 15 * time.Second
	resp, err := retryClient.Post(
		client.httpEndpoint,
		"application/json",
		reqBody,
	)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(client.getChainName),
			err,
			META_FAILED_CHAIN_NAME,
		).ConsoleLog()
	}

	v := &rpc.JsonRpcResult{}
	jsonDecodeErr := json.NewDecoder(resp.Body).Decode(&v)
	if jsonDecodeErr != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(client.getChainName),
			jsonDecodeErr,
			META_FAILED_CHAIN_DECODE,
		).ConsoleLog()
	}

	return v.Result.(string)
}

func (client *MetadataClient) UpdateLastProcessedHeight(blockHeight int) {
	go func() {
		client.pgClient.updateLastProcessedHeight(blockHeight)
	}()
}

func (client *MetadataClient) UpdateTargetHeight(blockHeight int) {
	go func() {
		client.pgClient.updateTargetHeight(blockHeight)
	}()
}

func (client *MetadataClient) SetRowCountEstimates() []RowCountEstimate {
	tablesEstimates := client.getRowCountEstimates()
	client.pgClient.updateRowsEstimate(tablesEstimates)
	return tablesEstimates
}

func (client *MetadataClient) UpdateRowCountEstimates(tableEstimates []RowCountEstimate) {
	client.pgClient.updateRowsEstimate(tableEstimates)
}

func (client *MetadataClient) SetIndexerHealthy(healthy bool) {
	client.pgClient.setIndexerHealthy(healthy)
}

func (client *MetadataClient) SetIndexerVersion(version string) {
	client.pgClient.setIndexerVersion(version)
}
