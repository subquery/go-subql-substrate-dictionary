package main

import (
	"flag"
	"fmt"
	"go-dictionary/internal/clients/specversion"
	"go-dictionary/internal/config"
	"go-dictionary/internal/db/postgres"
	"go-dictionary/internal/db/rocksdb"
	"go-dictionary/internal/messages"

	metac "go-dictionary/internal/clients/metadata"
)

func main() {
	var (
		configFilePath          string
		dictionaryConfiguration config.Config
		dictionaryMessage       *messages.DictionaryMessage
		rdbClient               *rocksdb.RockClient
		pgClient                *postgres.PostgresClient
		lastBlock               int // last block synced by the node we interrogate
	)

	// load config
	flag.StringVar(&configFilePath, "configfile", "", "path to config file")
	flag.Parse()
	if configFilePath == "" {
		messages.NewDictionaryMessage(messages.LOG_LEVEL_INFO, "", nil, messages.CONFIG_NO_CUSTOM_PATH_SPECIFIED).ConsoleLog()
		dictionaryConfiguration, dictionaryMessage = config.LoadConfig(nil)
	} else {
		dictionaryConfiguration, dictionaryMessage = config.LoadConfig(&configFilePath)
	}

	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		return
	}

	// Rocksdb connect
	rdbClient, dictionaryMessage = rocksdb.OpenRocksdb(
		dictionaryConfiguration.RocksdbConfig.RocksdbPath,
		dictionaryConfiguration.RocksdbConfig.RocksdbSecondaryPath,
	)
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		return
	}
	defer rdbClient.Close()

	lastBlock, dictionaryMessage = rdbClient.GetLastBlockSynced()
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		return
	}

	// Postgres connect
	pgClient, dictionaryMessage = postgres.Connect(dictionaryConfiguration.PostgresConfig)
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		return
	}

	// SPEC VERSION -- spec version and ranges for each spec
	specVersionClient := specversion.NewSpecVersionClient(
		dictionaryConfiguration.SpecVersionConfig.FirstSpecVersion,
		lastBlock,
		rdbClient,
		pgClient,
		dictionaryConfiguration.ConnectionConfig.HttpRpcEndpoint,
	)

	specVersionsRange, dictionaryMessage := specVersionClient.Run()
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		return
	}

	// METADATA -- meta for spec version
	metadataClient := metac.NewMetadataClient(
		rdbClient,
		dictionaryConfiguration.ConnectionConfig.HttpRpcEndpoint,
	)

	specVersionMetadataMap, dictionaryMessage := metadataClient.GetMetadata(specVersionsRange)
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		return
	}

	for k, v := range specVersionMetadataMap {
		fmt.Println(k, v.MetaInstant)
	}

}
