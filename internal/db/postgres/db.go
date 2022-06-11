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
func Connect(dbConfiguration config.PostgresConfig) *PostgresClient {
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
		fmt.Sprintf("%s:%s/%s",
			dbConfiguration.Host,
			dbConfiguration.Port,
			dbConfiguration.Db,
		),
	).ConsoleLog()

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(Connect),
			err,
			messages.POSTGRES_FAILED_TO_PARSE_CONNECTION_STRING,
		).ConsoleLog()
	}

	poolConnection, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(Connect),
			err,
			messages.POSTGRES_FAILED_TO_CONNECT,
		).ConsoleLog()
	}

	err = poolConnection.Ping(context.Background())
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(Connect),
			err,
			messages.POSTGRES_FAILED_TO_PING,
		).ConsoleLog()
	}

	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_SUCCESS,
		"",
		nil,
		messages.POSTGRES_CONNECTED,
	).ConsoleLog()
	return &PostgresClient{Pool: poolConnection}
}

func (pc *PostgresClient) Close() {
	pc.Pool.Close()
}
