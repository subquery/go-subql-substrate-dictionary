package specversion

import (
	"context"
	"fmt"
	"go-dictionary/internal/messages"

	"github.com/jackc/pgx/v4"
)

const (
	tableSpecVersionName  = "spec_versions"
	columnIdName          = "id"
	columnBlockHeightName = "block_height"
)

// getLastSolvedBlockAndSpecVersion gets from the db the last block for which we managed to get it's spec version
func (client *specvRepoClient) getLastSolvedBlockAndSpecVersion() *SpecVersionRange {
	var (
		err   error
		specV SpecVersionRange
	)

	query := fmt.Sprintf(
		"SELECT * FROM %s ORDER BY %s DESC LIMIT 1",
		tableSpecVersionName,
		columnBlockHeightName,
	)

	err = client.Pool.
		QueryRow(context.Background(), query).
		Scan(
			&specV.SpecVersion,
			&specV.First,
		)
	if err != nil {
		if err == pgx.ErrNoRows {
			messages.NewDictionaryMessage(
				messages.LOG_LEVEL_INFO,
				"",
				nil,
				SPEC_VERSION_NO_PREVIOUS_WORK,
			).ConsoleLog()
			return &SpecVersionRange{
				SpecVersion: "-1",
				First:       -1,
			}
		}

		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(client.getLastSolvedBlockAndSpecVersion),
			err,
			SPEC_VERSION_FAILED_DB_LAST_BLOCK,
		).ConsoleLog()
	}
	return &specV
}

// getAllSpecVersionData retrieves all the spec versions info from db
func (client *specvRepoClient) getAllSpecVersionData() SpecVersionRangeList {
	var (
		err         error
		specVData   []SpecVersionRange
		rows        pgx.Rows
		id          string
		blockHeight int
	)

	query := fmt.Sprintf(
		"SELECT * FROM %s ORDER BY %s ASC",
		tableSpecVersionName,
		columnBlockHeightName,
	)

	rows, err = client.Pool.Query(context.Background(), query)
	if err != nil {
		// we already checked before calling this that there is data in db
		if err == pgx.ErrNoRows {
			messages.NewDictionaryMessage(
				messages.LOG_LEVEL_ERROR,
				messages.GetComponent(client.getLastSolvedBlockAndSpecVersion),
				err,
				SPEC_VERSION_NO_PREVIOUS_WORK,
			).ConsoleLog()
		}

		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(client.getLastSolvedBlockAndSpecVersion),
			err,
			SPEC_VERSION_FAILED_DB,
		).ConsoleLog()
	}

	// no defer rows.Close() as Next() automatically closes them
	for rows.Next() {
		rows.Scan(
			&id,
			&blockHeight,
		)
		specVData = append(specVData, SpecVersionRange{SpecVersion: id, First: blockHeight})
	}

	return specVData
}

// insertSpecVersionsList inserts in a transaction the spec version list received as a parameter;
// oldSpecVersion represents the last spec version and it's block height that exists in the database,
// if there was none inserted in the db prior to the dictionary builder current run, a nil pointer will
// be sent
func (client *specvRepoClient) insertSpecVersionsList(
	newSpecVersions SpecVersionRangeList,
) {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		SPEC_VERSION_DB_INSERT,
		newSpecVersions[0].First,
	).ConsoleLog()

	tx, err := client.Pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(client.insertSpecVersionsList),
			err,
			messages.POSTGRES_FAILED_TO_START_TRANSACTION,
		).ConsoleLog()
	}
	defer tx.Rollback(context.Background())

	insertRows := [][]interface{}{}
	for _, svModel := range newSpecVersions {
		insertRows = append(insertRows, []interface{}{
			svModel.SpecVersion,
			svModel.First,
		})
	}

	copyLen, err := tx.CopyFrom(
		context.Background(),
		pgx.Identifier{tableSpecVersionName},
		[]string{columnIdName, columnBlockHeightName},
		pgx.CopyFromRows(insertRows),
	)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(client.insertSpecVersionsList),
			err,
			messages.POSTGRES_FAILED_TO_COPY_FROM,
		).ConsoleLog()
	}
	if copyLen != int64(len(insertRows)) {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(client.insertSpecVersionsList),
			fmt.Errorf(messages.POSTGRES_WRONG_NUMBER_OF_COPIED_ROWS, copyLen, len(insertRows)),
			"",
		).ConsoleLog()
	}

	err = tx.Commit(context.Background())
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(client.insertSpecVersionsList),
			err,
			messages.POSTGRES_FAILED_TO_COMMIT_TX,
		).ConsoleLog()
	}

	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_SUCCESS,
		"",
		nil,
		SPEC_VERSION_DB_SUCCESS,
	).ConsoleLog()
}
