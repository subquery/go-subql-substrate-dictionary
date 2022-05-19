package extrinsic

import (
	"context"
	"fmt"
	"go-dictionary/internal/messages"

	"github.com/jackc/pgx/v4"
)

// insertExtrinsic is called by other functions to insert a single extrinsic in the db chan
func (repoClient *extrinsicRepoClient) insertExtrinsic(extrinsic *Extrinsic) {
	repoClient.dbChan <- extrinsic
}

func (repoClient *extrinsicRepoClient) startDbWorker() {
	insertItems := make([][]interface{}, repoClient.batchSize)
	counter := 0

	for extrinsic := range repoClient.dbChan {
		insertItems[counter] = []interface{}{
			extrinsic.Id,
			extrinsic.TxHash,
			extrinsic.Module,
			extrinsic.Call,
			extrinsic.BlockHeight,
			extrinsic.Success,
			extrinsic.IsSigned,
		}
		counter++
		if counter == repoClient.batchSize {
			repoClient.insertBatch(insertItems)
			counter = 0
		}
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
		panic(nil)
	}
	defer tx.Rollback(context.Background())

	copyLen, err := tx.CopyFrom(
		context.Background(),
		pgx.Identifier{"extrinsics"},
		[]string{
			"id",
			"tx_hash",
			"module",
			"call",
			"block_height",
			"success",
			"is_signed",
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
		panic(nil)
	}
	if copyLen != int64(len(batch)) {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.insertBatch),
			fmt.Errorf(messages.POSTGRES_WRONG_NUMBER_OF_COPIED_ROWS, copyLen, len(batch)),
			"",
		).ConsoleLog()
		panic(nil)
	}

	err = tx.Commit(context.Background())
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.insertBatch),
			err,
			messages.POSTGRES_FAILED_TO_COMMIT_TX,
		).ConsoleLog()
		panic(nil)
	}
}
