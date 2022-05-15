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
func (client *specvRepoClient) getLastSolvedBlockAndSpecVersion() (*SpecVersionRange, *messages.DictionaryMessage) {
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
			&specV.Last,
		)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, messages.NewDictionaryMessage(
				messages.LOG_LEVEL_INFO,
				"",
				nil,
				messages.SPEC_VERSION_NO_PREVIOUS_WORK,
			)
		}

		return nil, messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(client.getLastSolvedBlockAndSpecVersion),
			err,
			messages.SPEC_VERSION_FAILED_DB_LAST_BLOCK,
		)
	}

	return &specV, nil
}

// getAllSpecVersionData retrieves all the spec versions info from db
func (client *specvRepoClient) getAllSpecVersionData() (SpecVersionRangeList, *messages.DictionaryMessage) {
	var (
		err         error
		specVData   []SpecVersionRange
		rows        pgx.Rows
		id          string
		blockHeight int
	)

	query := fmt.Sprintf(
		"SELECT * FROM %s",
		tableSpecVersionName,
	)

	rows, err = client.Pool.Query(context.Background(), query)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, messages.NewDictionaryMessage(
				messages.LOG_LEVEL_INFO,
				"",
				nil,
				messages.SPEC_VERSION_NO_PREVIOUS_WORK,
			)
		}

		return nil, messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(client.getLastSolvedBlockAndSpecVersion),
			err,
			messages.SPEC_VERSION_FAILED_DB,
		)
	}

	// no defer rows.Close() as Next() automatically closes them
	for rows.Next() == true {
		rows.Scan(
			&id,
			&blockHeight,
		)
		specVData = append(specVData, SpecVersionRange{SpecVersion: id, Last: blockHeight})
	}

	return specVData, nil
}

// insertSpecVersionsList inserts in a transaction the spec version list received as a parameter;
// oldSpecVersion represents the last spec version and it's block height that exists in the database,
// if there was none inserted in the db prior to the dictionary builder current run, a nil pointer will
// be sent
func (client *specvRepoClient) insertSpecVersionsList(
	newSpecVersions SpecVersionRangeList,
	oldSpecVersion *SpecVersionRange) *messages.DictionaryMessage {

	var firstBlock int
	if oldSpecVersion != nil {
		firstBlock = oldSpecVersion.Last
	} else {
		firstBlock = newSpecVersions[0].First
	}
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		messages.SPEC_VERSION_DB_INSERT,
		firstBlock,
	).ConsoleLog()

	tx, err := client.Pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		return messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(client.insertSpecVersionsList),
			err,
			messages.POSTGRES_FAILED_TO_START_TRANSACTION,
		)
	}
	defer tx.Rollback(context.Background())

	toBeInserted := newSpecVersions

	if oldSpecVersion != nil && newSpecVersions[0].SpecVersion == oldSpecVersion.SpecVersion {
		updateOldQuery := fmt.Sprintf(
			"UPDATE %s SET %s=$1 WHERE %s=$2",
			tableSpecVersionName,
			columnBlockHeightName,
			columnIdName,
		)

		_, err = tx.Exec(
			context.Background(),
			updateOldQuery,
			newSpecVersions[0].Last,
			newSpecVersions[0].SpecVersion,
		)
		if err != nil {
			return messages.NewDictionaryMessage(
				messages.LOG_LEVEL_ERROR,
				messages.GetComponent(client.insertSpecVersionsList),
				err,
				messages.POSTGRES_FAILED_TO_EXECUTE_UPDATE,
			)
		}
		toBeInserted = toBeInserted[1:]
	}

	insertRows := [][]interface{}{}
	for _, svModel := range toBeInserted {
		insertRows = append(insertRows, []interface{}{
			svModel.SpecVersion,
			svModel.Last,
		})
	}

	copyLen, err := tx.CopyFrom(
		context.Background(),
		pgx.Identifier{tableSpecVersionName},
		[]string{columnIdName, columnBlockHeightName},
		pgx.CopyFromRows(insertRows),
	)
	if err != nil {
		return messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(client.insertSpecVersionsList),
			err,
			messages.POSTGRES_FAILED_TO_COPY_FROM,
		)
	}
	if copyLen != int64(len(insertRows)) {
		return messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(client.insertSpecVersionsList),
			fmt.Errorf(messages.POSTGRES_WRONG_NUMBER_OF_COPIED_ROWS, copyLen, len(insertRows)),
			"",
		)
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(client.insertSpecVersionsList),
			err,
			messages.POSTGRES_FAILED_TO_COMMIT_TX,
		)
	}

	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_SUCCESS,
		"",
		nil,
		messages.SPEC_VERSION_DB_SUCCESS,
	).ConsoleLog()

	return nil
}
