package clients

import (
	"go-dictionary/internal/clients/event"
	"go-dictionary/internal/clients/extrinsic"
	"go-dictionary/internal/clients/specversion"
	"go-dictionary/internal/config"
	"go-dictionary/internal/db/postgres"
	"go-dictionary/internal/db/rocksdb"
	"go-dictionary/internal/messages"
	"io/ioutil"
	"sync/atomic"

	"github.com/itering/scale.go/source"
	"github.com/itering/scale.go/types"
)

const (
	firstChainBlock = 1
)

type (
	Orchestrator struct {
		configuration      config.Config
		pgClient           *postgres.PostgresClient
		rdbClient          *rocksdb.RockClient
		specversionClient  *specversion.SpecVersionClient
		extrinsicClient    *extrinsic.ExtrinsicClient
		eventClient        *event.EventClient
		extrinsicHeight    uint64
		lastProcessedBlock int
	}
)

// Neworchestrator creates and initialises a new orchestrator
func NewOrchestrator(
	config config.Config,
) *Orchestrator {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		ORCHESTRATOR_INITIALIZING,
	).ConsoleLog()

	// Postgres connect
	pgClient := postgres.Connect(config.PostgresConfig)

	// Rocksdb connect
	rdbClient := rocksdb.OpenRocksdb(config.RocksdbConfig)
	lastBlock := rdbClient.GetLastBlockSynced()

	// SPEC VERSION -- spec version and ranges for each spec
	specVersionClient := specversion.NewSpecVersionClient(
		lastBlock,
		rdbClient,
		pgClient,
		config.ChainConfig.HttpRpcEndpoint,
	)
	specVersionClient.Run()

	// Register custom types
	c, err := ioutil.ReadFile(config.ChainConfig.DecoderTypesFile)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(NewOrchestrator),
			err,
			ORCHESTRATOR_FAILED_TO_READ_TYPES_FILE,
		).ConsoleLog()
	}
	types.RegCustomTypes(source.LoadTypeRegistry(c))

	// EXTRINSIC - extrinsic client
	extrinsicClient := extrinsic.NewExtrinsicClient(
		pgClient,
		rdbClient,
		config.ClientsConfig.Extrinsics.Workers,
		specVersionClient,
	)
	extrinsicClient.Run()
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		ORCHESTRATOR_BATCH_SIZE,
		"Extrinsic",
		config.ClientsConfig.Extrinsics.BatchSize,
	).ConsoleLog()

	// EVENTS - event client
	eventClient := event.NewEventClient(
		pgClient,
		rdbClient,
		config.ClientsConfig.Events.Workers,
		specVersionClient,
	)
	eventClient.Run()
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		ORCHESTRATOR_BATCH_SIZE,
		"Event",
		config.ClientsConfig.Events.BatchSize,
	).ConsoleLog()

	return &Orchestrator{
		configuration:      config,
		pgClient:           pgClient,
		rdbClient:          rdbClient,
		specversionClient:  specVersionClient,
		extrinsicClient:    extrinsicClient,
		eventClient:        eventClient,
		lastProcessedBlock: lastBlock,
	}
}

func (orchestrator *Orchestrator) Run() {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		ORCHESTRATOR_START,
	).ConsoleLog()

	go orchestrator.runExtrinsics()
	go orchestrator.runEvents()

	c := make(chan struct{})
	<-c
}

func (orchestrator *Orchestrator) runExtrinsics() {
	workerName := "EXTRINSIC"

	var extrinsicBatchChannel *extrinsic.ExtrinsicBatchChannel
	extrinsicBatchChannel = orchestrator.extrinsicClient.StartBatch()
	startingBlock := orchestrator.extrinsicClient.RecoverLastInsertedBlock()
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		ORCHESTRATOR_START_PROCESSING,
		workerName,
		startingBlock,
	).ConsoleLog()

	atomic.StoreUint64(&orchestrator.extrinsicHeight, uint64(startingBlock))

	for {
		lastBlock := orchestrator.getLastSyncedBlock()
		for blockHeight := startingBlock + 1; blockHeight <= lastBlock; blockHeight++ {
			lookupKey := orchestrator.rdbClient.GetLookupKeyForBlockHeight(blockHeight)
			extrinsicBatchChannel.SendWork(blockHeight, lookupKey)

			if blockHeight%orchestrator.configuration.ClientsConfig.Extrinsics.BatchSize == 0 {
				extrinsicBatchChannel.Close()
				orchestrator.extrinsicClient.WaitForBatchDbInsertion()
				atomic.StoreUint64(&orchestrator.extrinsicHeight, uint64(blockHeight))
				extrinsicBatchChannel = orchestrator.extrinsicClient.StartBatch()

				messages.NewDictionaryMessage(
					messages.LOG_LEVEL_SUCCESS,
					"",
					nil,
					ORCHESTRATOR_FINISH_BATCH,
					workerName,
					blockHeight,
				).ConsoleLog()
			}
		}

		if lastBlock%orchestrator.configuration.ClientsConfig.Extrinsics.BatchSize != 0 {
			extrinsicBatchChannel.Close()
			orchestrator.extrinsicClient.WaitForBatchDbInsertion()
			atomic.StoreUint64(&orchestrator.extrinsicHeight, uint64(lastBlock))
			extrinsicBatchChannel = orchestrator.extrinsicClient.StartBatch()

			messages.NewDictionaryMessage(
				messages.LOG_LEVEL_SUCCESS,
				"",
				nil,
				ORCHESTRATOR_FINISH_BATCH,
				workerName,
				lastBlock,
			).ConsoleLog()
		}

		startingBlock = lastBlock
	}
}

func (orchestrator *Orchestrator) runEvents() {
	workerName := "EVENT"

	eventBatchChannel := orchestrator.eventClient.StartBatch()
	lastProcessedEvent := orchestrator.eventClient.RecoverLastInsertedBlock()
	var lastExtrinsicBlockHeight int
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		ORCHESTRATOR_START_PROCESSING,
		workerName,
		lastProcessedEvent,
	).ConsoleLog()

	for {
		lastExtrinsicBlockHeight = int(atomic.LoadUint64(&orchestrator.extrinsicHeight))
		if lastExtrinsicBlockHeight <= lastProcessedEvent {
			continue
		}

		for blockHeight := lastProcessedEvent + 1; blockHeight <= lastExtrinsicBlockHeight; blockHeight++ {
			lookupKey := orchestrator.rdbClient.GetLookupKeyForBlockHeight(blockHeight)
			eventBatchChannel.SendWork(blockHeight, lookupKey)

			if blockHeight%orchestrator.configuration.ClientsConfig.Events.BatchSize == 0 {
				eventBatchChannel.Close()
				orchestrator.eventClient.WaitForBatchDbInsertion()
				eventBatchChannel = orchestrator.eventClient.StartBatch()

				messages.NewDictionaryMessage(
					messages.LOG_LEVEL_SUCCESS,
					"",
					nil,
					ORCHESTRATOR_FINISH_BATCH,
					workerName,
					blockHeight,
				).ConsoleLog()
			}
		}

		if lastExtrinsicBlockHeight%orchestrator.configuration.ClientsConfig.Events.BatchSize != 0 {
			eventBatchChannel.Close()
			orchestrator.eventClient.WaitForBatchDbInsertion()
			eventBatchChannel = orchestrator.eventClient.StartBatch()

			messages.NewDictionaryMessage(
				messages.LOG_LEVEL_SUCCESS,
				"",
				nil,
				ORCHESTRATOR_FINISH_BATCH,
				workerName,
				lastExtrinsicBlockHeight,
			).ConsoleLog()
		}

		lastProcessedEvent = lastExtrinsicBlockHeight
	}
}

func (orchestrator *Orchestrator) Close() {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		ORCHESTRATOR_CLOSE,
	).ConsoleLog()

	orchestrator.rdbClient.Close()
	orchestrator.pgClient.Close()
}

func (orchestrator *Orchestrator) getLastSyncedBlock() int {
	for {
		orchestrator.rdbClient.CatchUpWithPrimary()
		lastBlock := orchestrator.rdbClient.GetLastBlockSynced()
		if lastBlock != orchestrator.lastProcessedBlock && lastBlock >= firstChainBlock {
			orchestrator.specversionClient.UpdateLive(lastBlock)
			orchestrator.lastProcessedBlock = lastBlock
			return lastBlock
		}
	}
}
