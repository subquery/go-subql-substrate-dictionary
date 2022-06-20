package extrinsic

import (
	"context"
	"fmt"
	"go-dictionary/internal/messages"
	"go-dictionary/models"

	"github.com/jackc/pgx/v4"
)

const (
	insertBufferInitialSize  = 2000
	tableExtrinsicName       = "extrinsics"
	tableEvmTransactionsName = "evm_transactions"
	colId                    = "id"
	colTxHash                = "tx_hash"
	colFrom                  = "from"
	colTo                    = "to"
	colModule                = "module"
	colCall                  = "call"
	colFunc                  = "func"
	colBlockHeight           = "block_height"
	colSuccess               = "success"
	colIsSigned              = "is_signed"
)

// insertExtrinsic is called by other functions to insert a single extrinsic in the db chan
func (repoClient *extrinsicRepoClient) insertExtrinsic(extrinsic interface{}) {
	repoClient.dbChan <- extrinsic
}

func (repoClient *extrinsicRepoClient) evmStartDbWorker() {
	insertExtrinsics := make([][]interface{}, insertBufferInitialSize)
	insertEvmTransactions := make([][]interface{}, insertBufferInitialSize)
	workerCounter := 0
	extrinsicsCounter := 0
	evmTransactionsCounter := 0

	for extrinsicRaw := range repoClient.dbChan {
		if extrinsicRaw == nil {
			workerCounter++
			// if all workers finished the current batch processing, insert in db
			if workerCounter == repoClient.workersCount {
				repoClient.evmInsertBatch(insertExtrinsics[:extrinsicsCounter], insertEvmTransactions[:evmTransactionsCounter])
				extrinsicsCounter = 0
				workerCounter = 0
				evmTransactionsCounter = 0
				repoClient.batchFinishedChan <- struct{}{} // send batch inserted signal
			}
			continue
		}

		switch extrinsic := extrinsicRaw.(type) {
		case *Extrinsic:
			toBeInsertedExtrinsic := []interface{}{
				extrinsic.Id,
				extrinsic.TxHash,
				extrinsic.Module,
				extrinsic.Call,
				extrinsic.BlockHeight,
				extrinsic.Success,
				extrinsic.IsSigned,
			}

			if extrinsicsCounter < len(insertExtrinsics) {
				insertExtrinsics[extrinsicsCounter] = toBeInsertedExtrinsic
			} else {
				insertExtrinsics = append(insertExtrinsics, toBeInsertedExtrinsic)
			}
			extrinsicsCounter++
		case *models.EvmTransaction:
			toBeInsertedEvmTransactions := []interface{}{
				extrinsic.Id,
				extrinsic.TxHash,
				extrinsic.From,
				extrinsic.To,
				extrinsic.Func,
				extrinsic.BlockHeight,
				extrinsic.Success,
			}

			if evmTransactionsCounter < len(insertEvmTransactions) {
				insertEvmTransactions[evmTransactionsCounter] = toBeInsertedEvmTransactions
			} else {
				insertEvmTransactions = append(insertEvmTransactions, toBeInsertedEvmTransactions)
			}
			evmTransactionsCounter++
		}

	}
}

func (repoClient *extrinsicRepoClient) startDbWorker() {
	insertExtrinsics := make([][]interface{}, insertBufferInitialSize)
	workerCounter := 0
	extrinsicsCounter := 0

	for extrinsicRaw := range repoClient.dbChan {
		if extrinsicRaw == nil {
			workerCounter++
			// if all workers finished the current batch processing, insert in db
			if workerCounter == repoClient.workersCount {
				repoClient.insertBatch(insertExtrinsics[:extrinsicsCounter])
				extrinsicsCounter = 0
				workerCounter = 0
				repoClient.batchFinishedChan <- struct{}{} // send batch inserted signal
			}
			continue
		}

		extrinsic := extrinsicRaw.(*Extrinsic)
		toBeInsertedExtrinsic := []interface{}{
			extrinsic.Id,
			extrinsic.TxHash,
			extrinsic.Module,
			extrinsic.Call,
			extrinsic.BlockHeight,
			extrinsic.Success,
			extrinsic.IsSigned,
		}

		if extrinsicsCounter < len(insertExtrinsics) {
			insertExtrinsics[extrinsicsCounter] = toBeInsertedExtrinsic
		} else {
			insertExtrinsics = append(insertExtrinsics, toBeInsertedExtrinsic)
		}
		extrinsicsCounter++
	}
}

// evmInsertBatch inserts a batch of evm rows in a single transaction
func (repoClient *extrinsicRepoClient) evmInsertBatch(insertExtrinsics [][]interface{}, insertEvmTransactions [][]interface{}) {
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

	repoClient.copyExtrinsicsInTx(tx, insertExtrinsics)
	repoClient.copyEvmTransactionsInTx(tx, insertEvmTransactions)

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

// insertBatch inserts a batch of evm rows in a single transaction
func (repoClient *extrinsicRepoClient) insertBatch(insertExtrinsics [][]interface{}) {
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

	repoClient.copyExtrinsicsInTx(tx, insertExtrinsics)

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

func (repoClient *extrinsicRepoClient) copyExtrinsicsInTx(tx pgx.Tx, insertExtrinsics [][]interface{}) {
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
		pgx.CopyFromRows(insertExtrinsics),
	)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.copyExtrinsicsInTx),
			err,
			messages.POSTGRES_FAILED_TO_COPY_FROM,
		).ConsoleLog()
	}
	if copyLen != int64(len(insertExtrinsics)) {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.copyExtrinsicsInTx),
			fmt.Errorf(messages.POSTGRES_WRONG_NUMBER_OF_COPIED_ROWS, copyLen, len(insertExtrinsics)),
			"",
		).ConsoleLog()
	}
}

func (repoClient *extrinsicRepoClient) copyEvmTransactionsInTx(tx pgx.Tx, insertEvmTransactions [][]interface{}) {
	copyLen, err := tx.CopyFrom(
		context.Background(),
		pgx.Identifier{tableEvmTransactionsName},
		[]string{
			colId,
			colTxHash,
			colFrom,
			colTo,
			colFunc,
			colBlockHeight,
			colSuccess,
		},
		pgx.CopyFromRows(insertEvmTransactions),
	)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.copyEvmTransactionsInTx),
			err,
			messages.POSTGRES_FAILED_TO_COPY_FROM,
		).ConsoleLog()
	}
	if copyLen != int64(len(insertEvmTransactions)) {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(repoClient.copyEvmTransactionsInTx),
			fmt.Errorf(messages.POSTGRES_WRONG_NUMBER_OF_COPIED_ROWS, copyLen, len(insertEvmTransactions)),
			"",
		).ConsoleLog()
	}
}
