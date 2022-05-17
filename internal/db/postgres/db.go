package postgres

import (
	"context"
	"fmt"
	"go-dictionary/internal/config"
	"go-dictionary/internal/messages"
	"log"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
)

const (
	connStringFormat = "postgresql://%s:%s@%s:%s/%s?sslmode=disable&pool_max_conns=%d"
)

type (
	PostgresClient struct {
		Pool *pgxpool.Pool
	}
)

// Connect creates a new Postgres connection pool client instance
func Connect(dbConfiguration config.PostgresConfig) (*PostgresClient, *messages.DictionaryMessage) {
	connString := fmt.Sprintf(
		connStringFormat,
		dbConfiguration.User,
		dbConfiguration.Password,
		dbConfiguration.Host,
		dbConfiguration.Port,
		dbConfiguration.Db,
		dbConfiguration.ConnPool,
	)
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		messages.POSTGRES_CONNECTING,
		connString,
	).ConsoleLog()

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(Connect),
			err,
			messages.POSTGRES_FAILED_TO_PARSE_CONNECTION_STRING,
		)
	}

	poolConnection, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(Connect),
			err,
			messages.POSTGRES_FAILED_TO_CONNECT,
		)
	}

	err = poolConnection.Ping(context.Background())
	if err != nil {
		return nil, messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(Connect),
			err,
			messages.POSTGRES_FAILED_TO_PING,
		)
	}

	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_SUCCESS,
		"",
		nil,
		messages.POSTGRES_CONNECTED,
	).ConsoleLog()
	return &PostgresClient{Pool: poolConnection}, nil
}

func (pc *PostgresClient) Close() {
	pc.Pool.Close()
}

// func (pc *PostgresClient) EventsWorker(wg *sync.WaitGroup) {
// 	log.Println("[+] Starting EventsWorker!")
// 	defer wg.Done()
// 	maxBatch := 100000
// 	counter := 0
// 	insertItems := [][]interface{}{}
// 	for event := range pc.WorkersChannels.EventsChannel {
// 		insertItems = append(insertItems, []interface{}{event.Id, event.Module, event.Event, event.BlockHeight})
// 		counter++
// 		if counter == maxBatch {
// 			_, err := pc.Pool.CopyFrom(
// 				context.Background(),
// 				pgx.Identifier{"events"},
// 				[]string{"id", "module", "event", "block_height"},
// 				pgx.CopyFromRows(insertItems),
// 			)
// 			if err != nil {
// 				log.Println("[ERR]", err, "- could not insert items for events with CopyFrom!")
// 			} else {
// 				log.Println("[INFO] Inserted 100k events")
// 			}
// 			insertItems = nil
// 			counter = 0
// 		}
// 	}
// 	_, err := pc.Pool.CopyFrom(
// 		context.Background(),
// 		pgx.Identifier{"events"},
// 		[]string{"id", "module", "event", "block_height"},
// 		pgx.CopyFromRows(insertItems),
// 	)
// 	if err != nil {
// 		log.Println("[ERR]", err, "- could not insert items for events with CopyFrom!")
// 	} else {
// 		log.Println("[INFO] Inserted remaining events")
// 	}
// 	log.Println("[-] Exited EventsWorker...")
// }

// func (pc *PostgresClient) EvmLogsWorker(wg *sync.WaitGroup) {
// 	log.Println("[+] Started EvmLogsWorker!")
// 	defer wg.Done()
// 	maxBatch := 100000
// 	counter := 0
// 	insertItems := [][]interface{}{}
// 	for evmLog := range pc.WorkersChannels.EvmLogsChannel {
// 		insertItems = append(insertItems, []interface{}{evmLog.Id, evmLog.Address, evmLog.BlockHeight, evmLog.Topics0, evmLog.Topics1, evmLog.Topics2, evmLog.Topics3})
// 		counter++
// 		if counter == maxBatch {
// 			_, err := pc.Pool.CopyFrom(
// 				context.Background(),
// 				pgx.Identifier{"evm_logs"},
// 				[]string{"id", "address", "block_height", "topics0", "topics1", "topics2", "topics3"},
// 				pgx.CopyFromRows(insertItems),
// 			)
// 			if err != nil {
// 				log.Println("[ERR]", err, "- could not insert items for evmlogs with CopyFrom!")
// 			} else {
// 				log.Println("[INFO] Inserted 100k evm_logs")
// 			}
// 			insertItems = nil
// 			counter = 0
// 		}
// 	}
// 	_, err := pc.Pool.CopyFrom(
// 		context.Background(),
// 		pgx.Identifier{"evm_logs"},
// 		[]string{"id", "address", "block_height", "topics0", "topics1", "topics2", "topics3"},
// 		pgx.CopyFromRows(insertItems),
// 	)
// 	if err != nil {
// 		log.Println("[ERR]", err, "- could not insert items for evmlogs with CopyFrom!")
// 	} else {
// 		log.Println("[INFO] Inserted remaining evm_logs")
// 	}
// 	log.Println("[-] Exited EvmLogsWorker...")
// }

// func (pc *PostgresClient) EvmTransactionsWorker(wg *sync.WaitGroup) {
// 	log.Println("[+] Started EvmTransactionsWorker!")
// 	defer wg.Done()
// 	maxBatch := 100
// 	counter := 0
// 	insertItems := [][]interface{}{}
// 	for evmTransaction := range pc.WorkersChannels.EvmTransactionsChannel {
// 		insertItems = append(insertItems, []interface{}{evmTransaction.Id, evmTransaction.TxHash, evmTransaction.From, evmTransaction.To, evmTransaction.Func, evmTransaction.BlockHeight, evmTransaction.Success})
// 		counter++
// 		if counter == maxBatch {
// 			_, err := pc.Pool.CopyFrom(
// 				context.Background(),
// 				pgx.Identifier{"evm_transactions"},
// 				[]string{"id", "tx_hash", "from", "to", "func", "block_height", "success"},
// 				pgx.CopyFromRows(insertItems),
// 			)
// 			if err != nil {
// 				log.Println("[ERR]", err, "- could not insert items for evm_transactions with CopyFrom!")
// 			} else {
// 				log.Println("[INFO] Inserted 100 evm_transactions")
// 			}
// 			insertItems = nil
// 			counter = 0
// 		}
// 	}
// 	_, err := pc.Pool.CopyFrom(
// 		context.Background(),
// 		pgx.Identifier{"evm_transactions"},
// 		[]string{"id", "tx_hash", "from", "to", "func", "block_height", "success"},
// 		pgx.CopyFromRows(insertItems),
// 	)
// 	if err != nil {
// 		log.Println("[ERR]", err, "- could not insert items for evm_transactions with CopyFrom!")
// 	} else {
// 		log.Println("[INFO] Inserted the remaining evm_transactions")
// 	}
// 	log.Println("[-] Exited EvmTransactionsWorker...")
// }

// func (pc *PostgresClient) ExtrinsicsWorker(wg *sync.WaitGroup) {
// 	log.Println("[+] Started ExtrinsicsWorker!")
// 	defer wg.Done()
// 	maxBatch := 100000
// 	counter := 0
// 	insertItems := [][]interface{}{}
// 	for extrinsic := range pc.WorkersChannels.ExtrinsicsChannel {
// 		insertItems = append(insertItems, []interface{}{extrinsic.Id, extrinsic.TxHash, extrinsic.Module, extrinsic.Call, extrinsic.BlockHeight, extrinsic.Success, extrinsic.IsSigned})
// 		counter++
// 		if counter == maxBatch {
// 			_, err := pc.Pool.CopyFrom(
// 				context.Background(),
// 				pgx.Identifier{"extrinsics"},
// 				[]string{"id", "tx_hash", "module", "call", "block_height", "success", "is_signed"},
// 				pgx.CopyFromRows(insertItems),
// 			)
// 			if err != nil {
// 				log.Println("[ERR]", err, "- could not insert items for extrinsics with CopyFrom!")
// 			} else {
// 				log.Println("[INFO] Inserted 100k extrinsics")
// 			}
// 			insertItems = nil
// 			counter = 0
// 		}
// 	}
// 	_, err := pc.Pool.CopyFrom(
// 		context.Background(),
// 		pgx.Identifier{"extrinsics"},
// 		[]string{"id", "tx_hash", "module", "call", "block_height", "success", "is_signed"},
// 		pgx.CopyFromRows(insertItems),
// 	)
// 	if err != nil {
// 		log.Println("[ERR]", err, "- could not insert items for extrinsics with CopyFrom!")
// 	} else {
// 		log.Println("[INFO] Inserted remaining extrinsics")
// 	}
// 	log.Println("[-] Exited ExtrinsicsWorker...")
// }

func (pc *PostgresClient) InsertByQuery(query string) error {
	_, err := pc.Pool.Exec(context.Background(), query)
	if err != nil {
		log.Println("[ERR]", err, "- could not insert to postgres by query!")
		return err
	}
	return nil
}
