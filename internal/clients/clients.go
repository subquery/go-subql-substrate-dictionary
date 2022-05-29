package clients

import (
	"go-dictionary/internal/clients/event"
	"go-dictionary/internal/clients/extrinsic"
	"go-dictionary/internal/clients/metadata"
	"go-dictionary/internal/clients/specversion"
	"go-dictionary/internal/config"
	"go-dictionary/internal/db/postgres"
	"go-dictionary/internal/db/rocksdb"
	"go-dictionary/internal/messages"
	"io/ioutil"
	"log"

	"github.com/itering/scale.go/source"
	"github.com/itering/scale.go/types"
)

type (
	Orchestrator struct {
		configuration      config.Config
		pgClient           *postgres.PostgresClient
		rdbClient          *rocksdb.RockClient
		lastBlock          int
		specversionClient  *specversion.SpecVersionClient
		specVersionRange   specversion.SpecVersionRangeList
		metadataClient     *metadata.MetadataClient
		specVersionMetaMap map[string]*metadata.DictionaryMetadata
		extrinsicClient    *extrinsic.ExtrinsicClient
		eventClient        *event.EventClient
	}
)

// Neworchestrator creates and initialises a new orchestrator
func NewOrchestrator(
	config config.Config,
) *Orchestrator {
	// Postgres connect
	pgClient, dictionaryMessage := postgres.Connect(config.PostgresConfig)
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		panic(nil)
	}

	// Rocksdb connect
	rdbClient, dictionaryMessage := rocksdb.OpenRocksdb(
		config.RocksdbConfig.RocksdbPath,
		config.RocksdbConfig.RocksdbSecondaryPath,
	)
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		panic(nil)
	}

	lastBlock, dictionaryMessage := rdbClient.GetLastBlockSynced()
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		panic(nil)
	}

	// SPEC VERSION -- spec version and ranges for each spec
	specVersionClient := specversion.NewSpecVersionClient(
		config.SpecVersionConfig.FirstSpecVersion,
		lastBlock,
		rdbClient,
		pgClient,
		config.ConnectionConfig.HttpRpcEndpoint,
	)

	specVersionsRange, dictionaryMessage := specVersionClient.Run()
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		panic(nil)
	}

	// METADATA -- meta for spec version
	metadataClient := metadata.NewMetadataClient(
		rdbClient,
		config.ConnectionConfig.HttpRpcEndpoint,
	)

	specVersionMetadataMap, dictionaryMessage := metadataClient.GetMetadata(specVersionsRange)
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		panic(nil)
	}

	// Register custom types
	//TODO: file path from config file
	c, err := ioutil.ReadFile("./network/polkadot.json")
	if err != nil {
		log.Println("[ERR] Failed to register types for network Polkadot:", err)
		return nil
	}
	types.RegCustomTypes(source.LoadTypeRegistry(c))

	// EXTRINSIC - extrinsic client
	extrinsicClient := extrinsic.NewExtrinsicClient(
		pgClient,
		rdbClient,
		config.WorkersConfig.ExtrinsicWorkers,
		specVersionsRange,
		specVersionMetadataMap,
	)
	extrinsicClient.Run()

	// EVENTS - event client
	eventClient := event.NewEventClient(
		pgClient,
		rdbClient,
		config.WorkersConfig.EventsWorkers,
		specVersionsRange,
		specVersionMetadataMap,
	)
	eventClient.Run()

	return &Orchestrator{
		configuration:      config,
		pgClient:           pgClient,
		rdbClient:          rdbClient,
		lastBlock:          lastBlock,
		specversionClient:  specVersionClient,
		specVersionRange:   specVersionsRange,
		metadataClient:     metadataClient,
		specVersionMetaMap: specVersionMetadataMap,
		extrinsicClient:    extrinsicClient,
		eventClient:        eventClient,
	}
}

func (orchestrator *Orchestrator) Run() {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		messages.ORCHESTRATOR_START,
	).ConsoleLog()

	var extrinsicBatchChannel *extrinsic.ExtrinsicBatchChannel
	extrinsicBatchChannel = orchestrator.extrinsicClient.StartBatch()

	var eventBatchChannel *event.EventBatchChannel
	eventBatchChannel = orchestrator.eventClient.StartBatch()

	startingBlock := orchestrator.extrinsicClient.RecoverLastInsertedBlock()

	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		messages.ORCHESTRATOR_START_EXTRINSIC_BATCH,
		orchestrator.configuration.WorkersConfig.ExtrinsicBatchSize,
		startingBlock,
	).ConsoleLog()

	for blockHeight := startingBlock; blockHeight <= orchestrator.lastBlock; blockHeight++ {
		if blockHeight%orchestrator.configuration.WorkersConfig.ExtrinsicBatchSize == 0 {
			extrinsicBatchChannel.Close()
			orchestrator.extrinsicClient.WaitForBatchDbInsertion()

			messages.NewDictionaryMessage(
				messages.LOG_LEVEL_SUCCESS,
				"",
				nil,
				messages.ORCHESTRATOR_FINISH_EXTRINSIC_BATCH,
			).ConsoleLog()

			extrinsicBatchChannel = orchestrator.extrinsicClient.StartBatch()

			messages.NewDictionaryMessage(
				messages.LOG_LEVEL_INFO,
				"",
				nil,
				messages.ORCHESTRATOR_START_EXTRINSIC_BATCH,
				orchestrator.configuration.WorkersConfig.ExtrinsicBatchSize,
				blockHeight,
			).ConsoleLog()
		}

		lookupKey, msg := orchestrator.rdbClient.GetLookupKeyForBlockHeight(blockHeight)
		if msg != nil {
			msg.ConsoleLog()
			panic(nil)
		}

		eventBatchChannel.SendWork(blockHeight, lookupKey)
		extrinsicBatchChannel.SendWork(blockHeight, lookupKey)
	}

	//TODO: show some messages
	extrinsicBatchChannel.Close()
	orchestrator.extrinsicClient.WaitForBatchDbInsertion()
}

func (orchestrator *Orchestrator) Close() {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		messages.ORCHESTRATOR_CLOSE,
	).ConsoleLog()

	orchestrator.rdbClient.Close()
	orchestrator.pgClient.Close()
}
