package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"go-dictionary/internal/messages"

	"github.com/jackc/pgx/v4"
)

const (
	tableName    = "_metadata"
	colKey       = "key"
	colValue     = "value"
	colCreatedAt = "createdAt"
	colUpdatedAt = "updatedAt"

	// metadata keys
	lastProcessedHeight    = "lastProcessedHeight"
	lastProcessedTimestamp = "lastProcessedTimestamp"
	targetHeight           = "targetHeight"
	chain                  = "chain"
	specName               = "specName"
	genesisHash            = "genesisHash"
	indexerHealthy         = "indexerHealthy"
	indexerNodeVersion     = "indexerNodeVersion"
	queryNodeVersion       = "queryNodeVersion"
	rowCountEstimate       = "rowCountEstimate"
	dynamicDatasources     = "dynamicDatasources"
)

var (
	metadataKeyToDataType = map[string]string{
		"lastProcessedHeight":    "int",
		"lastProcessedTimestamp": "string",
		"targetHeight":           "int",
		"chain":                  "string",
		"specName":               "string",
		"genesisHash":            "string",
		"indexerHealthy":         "bool",
		"indexerNodeVersion":     "string",
		"queryNodeVersion":       "string",
		"rowCountEstimate":       "int",
		"dynamicDatasources":     "string",
	}
)

func (repoClient *metadataRepoClient) initTables(
	tablesEstimates []RowCountEstimate,
	genesisHash string,
	chainName string,
	specNameV string,
) {
	query := fmt.Sprintf(
		`INSERT INTO %s(%s,%s,"%s","%s") VALUES
		($1,$2,$3,$4),
		($5,$6,$7,$8),
		($9,$10,$11,$12),
		($13,$14,$15,$16),
		($17,$18,$19,$20),
		($21,$22,$23,$24),
		($25,$26,$27,$28),
		($29,$30,$31,$32),
		($33,$34,$35,$36),
		($37,$38,$39,$40),
		($41,$42,$43,$44)
		ON CONFLICT (%s) DO NOTHING`,
		tableName,
		colKey, colValue, colCreatedAt, colUpdatedAt,
		colKey,
	)

	currentTimestamp, timestampString := getTimestamp()

	jsonString, _ := json.Marshal("")
	jsonBool, _ := json.Marshal(true)
	jsonNil, _ := json.Marshal(nil)
	genesisHashJSON, _ := json.Marshal(genesisHash)
	chainNameJSON, _ := json.Marshal(chainName)
	specNameJSON, _ := json.Marshal(specNameV)

	tableEstimatesJson, err := json.Marshal(tablesEstimates)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.initTables),
			err,
			META_FAILED_JSON_MARSHAL,
			"table estimates",
		).ConsoleLog()
	}

	_, err = repoClient.Pool.Exec(
		context.Background(), query,
		lastProcessedHeight, 0, timestampString, timestampString,
		lastProcessedTimestamp, currentTimestamp, timestampString, timestampString,
		targetHeight, 0, timestampString, timestampString,
		chain, chainNameJSON, timestampString, timestampString,
		specName, specNameJSON, timestampString, timestampString,
		genesisHash, genesisHashJSON, timestampString, timestampString,
		indexerHealthy, jsonBool, timestampString, timestampString,
		indexerNodeVersion, jsonString, timestampString, timestampString,
		queryNodeVersion, jsonString, timestampString, timestampString,
		rowCountEstimate, tableEstimatesJson, timestampString, timestampString,
		dynamicDatasources, jsonNil, timestampString, timestampString,
	)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.initTables),
			err,
			messages.POSTGRES_FAILED_TO_INSERT,
		).ConsoleLog()
	}
}

// getTablesName returns the table names used by the dictionary instance
func (repoClient *metadataRepoClient) getTablesName() []string {
	query := `SELECT tablename
	FROM pg_catalog.pg_tables
	WHERE schemaname != 'pg_catalog' AND 
		schemaname != 'information_schema';`

	rows, err := repoClient.Pool.Query(context.Background(), query)
	if err != nil {
		if err == pgx.ErrNoRows {
			messages.NewDictionaryMessage(
				messages.LOG_LEVEL_ERROR,
				messages.GetComponent(repoClient.getTablesName),
				err,
				META_NO_TABLES,
			).ConsoleLog()
		}

		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.getTablesName),
			err,
			META_FAILED_DB,
		).ConsoleLog()
	}

	tableNames := []string{}
	var tableName string
	for rows.Next() {
		rows.Scan(&tableName)
		tableNames = append(tableNames, tableName)
	}

	return tableNames
}

// getLastProcessedHeight return the last block height entirely finished by the dictionary indexer
func (repoClient *metadataRepoClient) updateLastProcessedHeight(blockHeight int) {
	updateQuery := `UPDATE _metadata SET 
		value=$1, "updatedAt"=$2 
		WHERE key='lastProcessedHeight'`

	timestampInt, timestampString := getTimestamp()
	_, err := repoClient.Pool.Exec(
		context.Background(),
		updateQuery,
		blockHeight,
		timestampString,
	)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.updateTargetHeight),
			err,
			messages.POSTGRES_FAILED_TO_EXECUTE_UPDATE,
		).ConsoleLog()
	}
	repoClient.updateLastProcessedTimestamp(timestampInt, timestampString)
}

// getTableRowsCount returns the number of rows inside a table
func (repoClient *metadataRepoClient) getTableRowsCount(tableName string) int {
	var rowCount int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)

	err := repoClient.Pool.QueryRow(
		context.Background(),
		query,
	).Scan(&rowCount)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0
		}

		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.getTableRowsCount),
			err,
			META_FAILED_DB,
		).ConsoleLog()
	}

	return rowCount
}

func (repoClient *metadataRepoClient) updateTargetHeight(height int) {
	query := `UPDATE _metadata SET 
		value=$1, "updatedAt"=$2 
		WHERE key='targetHeight'`

	timestampInt, timestampString := getTimestamp()
	_, err := repoClient.Pool.Exec(
		context.Background(),
		query,
		height,
		timestampString,
	)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.updateTargetHeight),
			err,
			messages.POSTGRES_FAILED_TO_EXECUTE_UPDATE,
		).ConsoleLog()
	}
	repoClient.updateLastProcessedTimestamp(timestampInt, timestampString)
}

func (repoClient *metadataRepoClient) setIndexerHealthy(healthy bool) {
	query := `UPDATE _metadata SET 
	value=$1, "updatedAt"=$2 
	WHERE key='indexerHealthy'`

	timestampInt, timestampString := getTimestamp()
	_, err := repoClient.Pool.Exec(
		context.Background(),
		query,
		healthy,
		timestampString,
	)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.setIndexerHealthy),
			err,
			messages.POSTGRES_FAILED_TO_EXECUTE_UPDATE,
		).ConsoleLog()
	}
	repoClient.updateLastProcessedTimestamp(timestampInt, timestampString)
}

func (repoClient *metadataRepoClient) updateLastProcessedTimestamp(timestampInt int, timestampString string) {
	query := `UPDATE _metadata SET 
	value=$1, "updatedAt"=$2
	WHERE key='lastProcessedTimestamp'`

	_, err := repoClient.Pool.Exec(
		context.Background(),
		query,
		timestampInt,
		timestampString,
	)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.updateLastProcessedTimestamp),
			err,
			messages.POSTGRES_FAILED_TO_EXECUTE_UPDATE,
		).ConsoleLog()
	}
}

func (repoClient *metadataRepoClient) updateRowsEstimate(tablesEstimates []RowCountEstimate) {
	query := `UPDATE _metadata SET
	value=$1, "updatedAt"=$2
	WHERE key='rowCountEstimate'`

	timestampInt, timestampString := getTimestamp()
	tableEstimatesJson, err := json.Marshal(tablesEstimates)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.initTables),
			err,
			META_FAILED_JSON_MARSHAL,
			"table estimates",
		).ConsoleLog()
	}

	_, err = repoClient.Pool.Exec(
		context.Background(),
		query,
		tableEstimatesJson,
		timestampString,
	)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.updateRowsEstimate),
			err,
			messages.POSTGRES_FAILED_TO_EXECUTE_UPDATE,
		).ConsoleLog()
	}
	repoClient.updateLastProcessedTimestamp(timestampInt, timestampString)
}
