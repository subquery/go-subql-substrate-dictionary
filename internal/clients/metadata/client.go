package metadata

import (
	"bytes"
	"encoding/json"
	"go-dictionary/internal/db/postgres"
	"go-dictionary/internal/db/rocksdb"
	"go-dictionary/internal/messages"
	"net/http"

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
	}
)

var (
	CHAIN_MESSAGE = []byte(`{"id":1,"method":"system_chain","params":[],"jsonrpc":"2.0"}`)
)

func NewMetadataClient(
	pgClient *postgres.PostgresClient,
	rocksdbClient *rocksdb.RockClient,
	httpEndpoint string,
) *MetadataClient {
	return &MetadataClient{
		pgClient: metadataRepoClient{
			pgClient,
		},
		rocksdbClient: rocksdbClient,
		httpEndpoint:  httpEndpoint,
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
	genesisHash := client.rocksdbClient.GetGenesisHash()
	chainName := client.getChainName()

	client.pgClient.initTables(
		tablesEstimates,
		"0x"+genesisHash,
		chainName,
	)

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
	client.pgClient.setIndexerHealthy(true)
	defer client.pgClient.setIndexerHealthy(false)
	for {
		client.pgClient.updateLastProcessedHeight()
		//TODO: update only row estimates here
		//TODO: update last processed height and target height in orchestrator(events)
	}
}

func (client *MetadataClient) getChainName() string {
	reqBody := bytes.NewBuffer(CHAIN_MESSAGE)
	resp, err := http.Post(
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
