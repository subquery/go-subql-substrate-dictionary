package event

import (
	"context"
	"fmt"
	"go-dictionary/internal/messages"
	"go-dictionary/models"

	"github.com/jackc/pgx/v4"
)

const (
	insertBufferInitialSize  = 2000
	tableEventName           = "events"
	tableEvmLogsName         = "evm_logs"
	tableEvmTransactionsName = "evm_transactions"
	colId                    = "id"
	colAddress               = "address"
	colModule                = "module"
	colEvent                 = "event"
	colBlockHeight           = "block_height"
	colTopics0               = "topics0"
	colTopics1               = "topics1"
	colTopics2               = "topics2"
	colTopics3               = "topics3"

	updateExtrinsicQuery      = "UPDATE extrinsics SET success=$1 WHERE id=$2"
	updateEvmTransactionQuery = `UPDATE evm_transactions SET tx_hash=$1, "from"=$2, "to"=$3, success=$4 WHERE id=$5`
)

// insertEvent inserts an event in the database insertion buffer channel
func (repoClient *eventRepoClient) insertEvent(event interface{}) {
	repoClient.dbChan <- event
}

func (repoClient *eventRepoClient) evmStartDbWorker() {
	insertEvents := make([][]interface{}, insertBufferInitialSize)
	insertEvmLogs := make([][]interface{}, insertBufferInitialSize)
	updateEvmTransactions := make([]UpdateEvmTransactions, insertBufferInitialSize)
	updateExtrinsics := make([]UpdateExtrinsic, insertBufferInitialSize)
	workerCounter := 0
	insertEventsCounter := 0
	insertEvmLogsCounter := 0
	updateEvmTransactionCounter := 0
	updateExtrinsicCounter := 0

	for eventRaw := range repoClient.dbChan {
		if eventRaw == nil {
			workerCounter++
			if workerCounter == repoClient.workersCount {
				repoClient.evmInsertBatch(
					insertEvents[:insertEventsCounter],
					updateExtrinsics[:updateExtrinsicCounter],
					updateEvmTransactions[:updateEvmTransactionCounter],
					insertEvmLogs[:insertEvmLogsCounter],
				)
				workerCounter = 0
				insertEventsCounter = 0
				insertEvmLogsCounter = 0
				updateEvmTransactionCounter = 0
				updateExtrinsicCounter = 0

				repoClient.batchFinishedChan <- struct{}{}
			}
			continue
		}

		switch event := eventRaw.(type) {
		case *Event:
			// we encoded in block height the db command
			if event.BlockHeight == updateExtrinsicCommand {
				toBeUpdatedExtrinsic := UpdateExtrinsic{
					Id:      event.Id,
					Success: false,
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

				if insertEventsCounter < len(insertEvents) {
					insertEvents[insertEventsCounter] = toBeInsertedEvent
				} else {
					insertEvents = append(insertEvents, toBeInsertedEvent)
				}
				insertEventsCounter++
			}
		case *EvmLog:
			toBeInsertedEvmLog := []interface{}{
				event.Id,
				event.Address,
				event.BlockHeight,
				event.Topics[0],
				event.Topics[1],
				event.Topics[2],
				event.Topics[3],
			}

			if insertEvmLogsCounter < len(insertEvmLogs) {
				insertEvmLogs[insertEvmLogsCounter] = toBeInsertedEvmLog
			} else {
				insertEvmLogs = append(insertEvmLogs, toBeInsertedEvmLog)
			}
			insertEvmLogsCounter++
		case *models.EvmTransaction:
			toBeUpdatedEvmTransaction := UpdateEvmTransactions{
				Id:      event.Id,
				TxHash:  event.TxHash,
				To:      event.To,
				From:    event.From,
				Success: event.Success,
			}

			if updateEvmTransactionCounter < len(updateEvmTransactions) {
				updateEvmTransactions[updateEvmTransactionCounter] = toBeUpdatedEvmTransaction
			} else {
				updateEvmTransactions = append(updateEvmTransactions, toBeUpdatedEvmTransaction)
			}
			updateEvmTransactionCounter++
		}

	}
}

func (repoClient *eventRepoClient) startDbWorker() {
	insertEvents := make([][]interface{}, insertBufferInitialSize)
	updateExtrinsics := make([]UpdateExtrinsic, insertBufferInitialSize)
	workerCounter := 0
	insertEventsCounter := 0
	updateExtrinsicCounter := 0

	for eventRaw := range repoClient.dbChan {
		if eventRaw == nil {
			workerCounter++
			if workerCounter == repoClient.workersCount {
				repoClient.insertBatch(
					insertEvents[:insertEventsCounter],
					updateExtrinsics[:updateExtrinsicCounter],
				)
				workerCounter = 0
				insertEventsCounter = 0
				updateExtrinsicCounter = 0

				repoClient.batchFinishedChan <- struct{}{}
			}
			continue
		}

		event := eventRaw.(*Event)
		// we encoded in block height the db command
		if event.BlockHeight == updateExtrinsicCommand {
			toBeUpdatedExtrinsic := UpdateExtrinsic{
				Id:      event.Id,
				Success: false,
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

			if insertEventsCounter < len(insertEvents) {
				insertEvents[insertEventsCounter] = toBeInsertedEvent
			} else {
				insertEvents = append(insertEvents, toBeInsertedEvent)
			}
			insertEventsCounter++
		}
	}
}

func (repoClient *eventRepoClient) evmInsertBatch(
	insertEvents [][]interface{},
	updateExtrinsics []UpdateExtrinsic,
	updateEvmTransactions []UpdateEvmTransactions,
	insertEvmLogs [][]interface{},
) {
	tx, err := repoClient.Pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.evmInsertBatch),
			err,
			messages.POSTGRES_FAILED_TO_START_TRANSACTION,
		).ConsoleLog()
	}
	defer tx.Rollback(context.Background())

	repoClient.copyEventsInTx(tx, insertEvents)
	repoClient.copyEvmLogsInTx(tx, insertEvmLogs)
	// Extrinsics Status
	if len(updateExtrinsics) > 0 {
		repoClient.updateExtrinsicsInTx(tx, updateExtrinsics)
	}
	// Update EvmTransactions
	if len(updateEvmTransactions) > 0 {
		repoClient.updateEvmTransactionsInTx(tx, updateEvmTransactions)
	}

	err = tx.Commit(context.Background())
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.evmInsertBatch),
			err,
			messages.POSTGRES_FAILED_TO_COMMIT_TX,
		).ConsoleLog()
	}
}

func (repoClient *eventRepoClient) insertBatch(
	insertEvents [][]interface{},
	updateExtrinsics []UpdateExtrinsic,
) {
	tx, err := repoClient.Pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.evmInsertBatch),
			err,
			messages.POSTGRES_FAILED_TO_START_TRANSACTION,
		).ConsoleLog()
	}
	defer tx.Rollback(context.Background())

	repoClient.copyEventsInTx(tx, insertEvents)
	if len(updateExtrinsics) > 0 {
		repoClient.updateExtrinsicsInTx(tx, updateExtrinsics)
	}

	err = tx.Commit(context.Background())
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.evmInsertBatch),
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

func (repoClient *eventRepoClient) copyEventsInTx(tx pgx.Tx, insertEvents [][]interface{}) {
	copyLen, err := tx.CopyFrom(
		context.Background(),
		pgx.Identifier{tableEventName},
		[]string{
			colId,
			colModule,
			colEvent,
			colBlockHeight,
		},
		pgx.CopyFromRows(insertEvents),
	)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.copyEventsInTx),
			err,
			messages.POSTGRES_FAILED_TO_COPY_FROM,
		).ConsoleLog()
	}
	if copyLen != int64(len(insertEvents)) {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.copyEventsInTx),
			fmt.Errorf(messages.POSTGRES_WRONG_NUMBER_OF_COPIED_ROWS, copyLen, len(insertEvents)),
			"",
		).ConsoleLog()
	}
}

func (repoClient *eventRepoClient) copyEvmLogsInTx(tx pgx.Tx, insertEvmLogs [][]interface{}) {
	copyLen, err := tx.CopyFrom(
		context.Background(),
		pgx.Identifier{tableEvmLogsName},
		[]string{
			colId,
			colAddress,
			colBlockHeight,
			colTopics0,
			colTopics1,
			colTopics2,
			colTopics3,
		},
		pgx.CopyFromRows(insertEvmLogs),
	)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.copyEvmLogsInTx),
			err,
			messages.POSTGRES_FAILED_TO_COPY_FROM,
		).ConsoleLog()
	}
	if copyLen != int64(len(insertEvmLogs)) {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.copyEvmLogsInTx),
			fmt.Errorf(messages.POSTGRES_WRONG_NUMBER_OF_COPIED_ROWS, copyLen, len(insertEvmLogs)),
			"",
		).ConsoleLog()
	}
}

func (repoClient *eventRepoClient) updateExtrinsicsInTx(tx pgx.Tx, updateExtrinsics []UpdateExtrinsic) {
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
			messages.GetComponent(repoClient.updateExtrinsicsInTx),
			err,
			messages.POSTGRES_FAILED_TO_EXECUTE_UPDATE,
		).ConsoleLog()
	}
	if commandTag.RowsAffected() != 1 {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.updateExtrinsicsInTx),
			nil,
			EVENT_WRONG_UPDATE_NUMBER,
			commandTag.RowsAffected(),
			1,
		).ConsoleLog()
	}
	batchResults.Close()
}

func (repoClient *eventRepoClient) updateEvmTransactionsInTx(tx pgx.Tx, updateEvmTransactions []UpdateEvmTransactions) {
	evmTransactionsUpdateBatch := &pgx.Batch{}
	for _, evmTransactionUpdate := range updateEvmTransactions {
		evmTransactionsUpdateBatch.Queue(
			updateEvmTransactionQuery,
			evmTransactionUpdate.TxHash,
			evmTransactionUpdate.From,
			evmTransactionUpdate.To,
			evmTransactionUpdate.Success,
			evmTransactionUpdate.Id,
		)
	}
	batchResults := tx.SendBatch(context.Background(), evmTransactionsUpdateBatch)
	commandTag, err := batchResults.Exec()
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.updateEvmTransactionsInTx),
			err,
			messages.POSTGRES_FAILED_TO_EXECUTE_UPDATE,
		).ConsoleLog()
	}
	if commandTag.RowsAffected() != 1 {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.updateEvmTransactionsInTx),
			nil,
			EVENT_WRONG_UPDATE_NUMBER,
			commandTag.RowsAffected(),
			1,
		).ConsoleLog()
	}
	batchResults.Close()
}
