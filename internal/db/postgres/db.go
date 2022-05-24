package postgres

import (
	"context"
	"fmt"
	"go-dictionary/internal/config"
	"go-dictionary/internal/messages"

	"github.com/jackc/pgx/v4/pgxpool"
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

// 		insertItems = append(insertItems, []interface{}{event.Id, event.Module, event.Event, event.BlockHeight})
// 	_, err := pc.Pool.CopyFrom(
// 		context.Background(),
// 		pgx.Identifier{"events"},
// 		[]string{"id", "module", "event", "block_height"},
// 		pgx.CopyFromRows(insertItems),
