package extrinsic

import (
	"context"
	"fmt"
	"go-dictionary/internal/messages"

	"github.com/jackc/pgx/v4"
)

const (
	insertBufferInitialSize = 2000
	tableExtrinsicName      = "extrinsics"
	colId                   = "id"
	colTxHash               = "tx_hash"
	colModule               = "module"
	colCall                 = "call"
	colBlockHeight          = "block_height"
	colSuccess              = "success"
	colIsSigned             = "is_signed"
)

// insertExtrinsic is called by other functions to insert a single extrinsic in the db chan
func (repoClient *extrinsicRepoClient) insertExtrinsic(extrinsic *Extrinsic) {
	repoClient.dbChan <- extrinsic
}

func (repoClient *extrinsicRepoClient) startDbWorker() {
	insertItems := make([][]interface{}, insertBufferInitialSize)
	workerCounter := 0
	counter := 0

	for extrinsic := range repoClient.dbChan {
		if extrinsic == nil {
			workerCounter++
			// if all workers finished the current batch processing, insert in db
			if workerCounter == repoClient.workersCount {
				repoClient.insertBatch(insertItems[:counter])
				counter = 0
				workerCounter = 0
				repoClient.batchFinishedChan <- struct{}{} // send batch inserted signal
			}
			continue
		}

		toBeInsertedExtrinsic := []interface{}{
			extrinsic.Id,
			extrinsic.TxHash,
			extrinsic.Module,
			extrinsic.Call,
			extrinsic.BlockHeight,
			extrinsic.Success,
			extrinsic.IsSigned,
		}

		if counter < len(insertItems) {
			insertItems[counter] = toBeInsertedExtrinsic
		} else {
			insertItems = append(insertItems, toBeInsertedExtrinsic)
		}
		counter++
	}
}

// insertBatch inserts a batch of rows in a single transaction
func (repoClient *extrinsicRepoClient) insertBatch(batch [][]interface{}) {
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
		pgx.Identifier{tableExtrinsicName},
		[]string{
			colId,
			colTxHash,
			colModule,
			colCall,
			colBlockHeight,
			colSuccess,
			colIsSigned,
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

func (repoClient *extrinsicRepoClient) recoverLastBlock() int {
	var blockHeight int

	query := fmt.Sprintf(
		"SELECT %s FROM %s ORDER BY %s DESC LIMIT 1",
		colBlockHeight,
		tableExtrinsicName,
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
			EXTRINSIC_FAILED_TO_RETRIEVE_LAST_BLOCK,
		).ConsoleLog()
	}

	return blockHeight
}
