package clients

import (
	"fmt"
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

type (
	Orchestrator struct {
		configuration     config.Config
		pgClient          *postgres.PostgresClient
		rdbClient         *rocksdb.RockClient
		lastBlock         int
		specversionClient *specversion.SpecVersionClient
		extrinsicClient   *extrinsic.ExtrinsicClient
		eventClient       *event.EventClient
		extrinsicHeight   uint64
	}
)

// Neworchestrator creates and initialises a new orchestrator
func NewOrchestrator(
	config config.Config,
) *Orchestrator {
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
			messages.ORCHESTRATOR_FAILED_TO_REGISTER_CUSTOM_DECODER_TYPES,
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

	// EVENTS - event client
	eventClient := event.NewEventClient(
		pgClient,
		rdbClient,
		config.ClientsConfig.Events.Workers,
		specVersionClient,
	)
	eventClient.Run()

	return &Orchestrator{
		configuration:     config,
		pgClient:          pgClient,
		rdbClient:         rdbClient,
		lastBlock:         lastBlock,
		specversionClient: specVersionClient,
		extrinsicClient:   extrinsicClient,
		eventClient:       eventClient,
	}
}

func (orchestrator *Orchestrator) Run() {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		messages.ORCHESTRATOR_START,
	).ConsoleLog()

	go orchestrator.runExtrinsics()
	go orchestrator.runEvents()

	c := make(chan struct{})
	<-c
}

func (orchestrator *Orchestrator) runExtrinsics() {
	var extrinsicBatchChannel *extrinsic.ExtrinsicBatchChannel
	extrinsicBatchChannel = orchestrator.extrinsicClient.StartBatch()
	startingBlock := orchestrator.extrinsicClient.RecoverLastInsertedBlock()
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		messages.ORCHESTRATOR_START_EXTRINSIC_BATCH,
		orchestrator.configuration.ClientsConfig.Extrinsics.BatchSize,
		startingBlock,
	).ConsoleLog()

	atomic.StoreUint64(&orchestrator.extrinsicHeight, uint64(startingBlock))

	for blockHeight := startingBlock + 1; blockHeight <= orchestrator.lastBlock; blockHeight++ {
		if blockHeight%orchestrator.configuration.ClientsConfig.Extrinsics.BatchSize == 0 {
			extrinsicBatchChannel.Close()
			orchestrator.extrinsicClient.WaitForBatchDbInsertion()
			atomic.StoreUint64(&orchestrator.extrinsicHeight, uint64(blockHeight-1))

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
				orchestrator.configuration.ClientsConfig.Extrinsics.BatchSize,
				blockHeight,
			).ConsoleLog()
		}

		lookupKey := orchestrator.rdbClient.GetLookupKeyForBlockHeight(blockHeight)

		extrinsicBatchChannel.SendWork(blockHeight, lookupKey)
	}

	//TODO: show some messages
	extrinsicBatchChannel.Close()
	orchestrator.extrinsicClient.WaitForBatchDbInsertion()
}

func (orchestrator *Orchestrator) runEvents() {
	eventBatchChannel := orchestrator.eventClient.StartBatch()
	lastProcessedEvent := orchestrator.eventClient.RecoverLastInsertedBlock()
	var lastExtrinsicBlockHeight int

	for {
		lastExtrinsicBlockHeight = int(atomic.LoadUint64(&orchestrator.extrinsicHeight))

		if lastProcessedEvent < lastExtrinsicBlockHeight {
			for blockHeight := lastProcessedEvent + 1; blockHeight <= lastExtrinsicBlockHeight; blockHeight++ {
				if blockHeight%orchestrator.configuration.ClientsConfig.Events.BatchSize == 0 {
					eventBatchChannel.Close()
					orchestrator.eventClient.WaitForBatchDbInsertion()
					lastProcessedEvent = blockHeight - 1
					eventBatchChannel = orchestrator.eventClient.StartBatch()
					fmt.Println("Finished events up to block ", lastProcessedEvent) //dbg
					//TODO: continue will jump over a block
					continue
				}

				lookupKey := orchestrator.rdbClient.GetLookupKeyForBlockHeight(blockHeight)

				eventBatchChannel.SendWork(blockHeight, lookupKey)
			}

			eventBatchChannel.Close()
			orchestrator.eventClient.WaitForBatchDbInsertion()
			lastProcessedEvent = lastExtrinsicBlockHeight
			eventBatchChannel = orchestrator.eventClient.StartBatch()
		}
	}
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
