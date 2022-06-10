package event

import (
	"context"
	"fmt"
	"go-dictionary/internal/messages"

	"github.com/jackc/pgx/v4"
)

const (
	insertBufferInitialSize = 2000
	tableEventName          = "events"
	colId                   = "id"
	colModule               = "module"
	colEvent                = "event"
	colBlockHeight          = "block_height"

	updateExtrinsicQuery = "UPDATE extrinsics SET success=$1 WHERE id=$2"
)

// insertEvent inserts an event in the database insertion buffer channel
func (repoClient *eventRepoClient) insertEvent(event *Event) {
	repoClient.dbChan <- event
}

func (repoClient *eventRepoClient) startDbWorker() {
	insertItems := make([][]interface{}, insertBufferInitialSize)
	updateExtrinsics := make([]UpdateExtrinsic, insertBufferInitialSize)
	workerCounter := 0
	insertCounter := 0
	updateExtrinsicCounter := 0

	for event := range repoClient.dbChan {
		if event == nil {
			workerCounter++
			if workerCounter == repoClient.workersCount {
				repoClient.insertBatch(insertItems[:insertCounter], updateExtrinsics[:updateExtrinsicCounter])
				workerCounter = 0
				insertCounter = 0
				updateExtrinsicCounter = 0
				repoClient.batchFinishedChan <- struct{}{}
			}
			continue
		}

		// we encoded in block height the db command
		if event.BlockHeight == updateExtrinsicCommand {
			toBeUpdatedExtrinsic := UpdateExtrinsic{
				Id:      event.Id,
				Success: getExtrinsicSuccess(event.Event),
			}

			if updateExtrinsicCounter < len(updateExtrinsics) {
				updateExtrinsics[updateExtrinsicCounter] = toBeUpdatedExtrinsic
			} else {
				updateExtrinsics = append(updateExtrinsics, toBeUpdatedExtrinsic)
			}
			updateExtrinsicCounter++
		} else {
			toBeInsertedEvent := []interface{}{
				event.Id,
				event.Module,
				event.Event,
				event.BlockHeight,
			}

			if insertCounter < len(insertItems) {
				insertItems[insertCounter] = toBeInsertedEvent
			} else {
				insertItems = append(insertItems, toBeInsertedEvent)
			}
			insertCounter++
		}
	}
}

func (repoClient *eventRepoClient) insertBatch(
	batch [][]interface{},
	updateExtrinsics []UpdateExtrinsic,
) {
	tx, err := repoClient.Pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.insertBatch),
			err,
			messages.POSTGRES_FAILED_TO_START_TRANSACTION,
		).ConsoleLog()
	}
	defer tx.Rollback(context.Background())

	copyLen, err := tx.CopyFrom(
		context.Background(),
		pgx.Identifier{tableEventName},
		[]string{
			colId,
			colModule,
			colEvent,
			colBlockHeight,
		},
		pgx.CopyFromRows(batch),
	)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.insertBatch),
			err,
			messages.POSTGRES_FAILED_TO_COPY_FROM,
		).ConsoleLog()
	}
	if copyLen != int64(len(batch)) {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.insertBatch),
			fmt.Errorf(messages.POSTGRES_WRONG_NUMBER_OF_COPIED_ROWS, copyLen, len(batch)),
			"",
		).ConsoleLog()
	}

	extrinsicUpdateBatch := &pgx.Batch{}
	for _, extrinsicUpdate := range updateExtrinsics {
		extrinsicUpdateBatch.Queue(
			updateExtrinsicQuery,
			extrinsicUpdate.Success,
			extrinsicUpdate.Id,
		)

	}
	batchResults := tx.SendBatch(context.Background(), extrinsicUpdateBatch)
	commandTag, err := batchResults.Exec()
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.insertBatch),
			err,
			messages.POSTGRES_FAILED_TO_EXECUTE_UPDATE,
		).ConsoleLog()
	}
	if commandTag.RowsAffected() != 1 {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.insertBatch),
			nil,
			EVENT_WRONG_UPDATE_NUMBER,
			commandTag.RowsAffected(),
			1,
		).ConsoleLog()
	}
	batchResults.Close()

	err = tx.Commit(context.Background())
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.insertBatch),
			err,
			messages.POSTGRES_FAILED_TO_COMMIT_TX,
		).ConsoleLog()
	}
}

func (repoClient *eventRepoClient) recoverLastBlock() int {
	var blockHeight int

	query := fmt.Sprintf(
		"SELECT %s FROM %s ORDER BY %s DESC LIMIT 1",
		colBlockHeight,
		tableEventName,
		colBlockHeight,
	)

	err := repoClient.Pool.
		QueryRow(context.Background(), query).
		Scan(&blockHeight)

	if err != nil {
		if err == pgx.ErrNoRows {
			return -1
		}

		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.recoverLastBlock),
			err,
			EVENT_FAILED_TO_RETRIEVE_LAST_BLOCK,
		).ConsoleLog()
	}

	return blockHeight
}

func getExtrinsicSuccess(eventCall string) bool {
	if eventCall == extrinsicSuccess {
		return true
	}

	if eventCall == extrinsicFailed {
		return false
	}

	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_ERROR,
		messages.GetComponent(getExtrinsicSuccess),
		nil,
		EVENT_UNKNOWN_EXTRINSIC_SUCCESS_STATUS,
		eventCall,
	).ConsoleLog()
	return false
}
