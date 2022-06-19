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
	"sync/atomic"

	scalecodec "github.com/itering/scale.go"
	"github.com/itering/scale.go/source"
	"github.com/itering/scale.go/types"
)

const (
	firstChainBlock = 1
)

type (
	Orchestrator struct {
		configuration              config.Config
		pgClient                   *postgres.PostgresClient
		rdbClient                  *rocksdb.RockClient
		specversionClient          *specversion.SpecVersionClient
		extrinsicClient            *extrinsic.ExtrinsicClient
		eventClient                *event.EventClient
		metadataClient             *metadata.MetadataClient
		extrinsicHeight            uint64
		lastProcessedBlock         int
		lastProcessedExtrinsicChan chan int
		metaDecode                 *scalecodec.MetadataDecoder
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
	if lastBlock == 0 {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(NewOrchestrator),
			nil,
			ORCHESTRATOR_FAILED_TO_GET_LAST_SYNCED_BLOCK,
		).ConsoleLog()
	}

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
	types.RuntimeType{}.Reg()
	types.RegCustomTypes(source.LoadTypeRegistry(c))

	// SPEC VERSION -- spec version and ranges for each spec
	specVersionClient := specversion.NewSpecVersionClient(
		lastBlock,
		rdbClient,
		pgClient,
		config.ChainConfig.HttpRpcEndpoint,
	)

	// quickfix
	// event.FixPolkdotEventDecoder()
	specVersionClient.Run()
	specName := specVersionClient.GetSpecName()

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

	// METADATA - metadata client
	metadataClient := metadata.NewMetadataClient(
		pgClient,
		rdbClient,
		config.ChainConfig.HttpRpcEndpoint,
	)
	metadataClient.Run(specName)
	metadataClient.SetIndexerHealthy(true)
	metadataClient.UpdateTargetHeight(lastBlock)

	lastExtrinsicChan := make(chan int, 20)

	return &Orchestrator{
		configuration:              config,
		pgClient:                   pgClient,
		rdbClient:                  rdbClient,
		specversionClient:          specVersionClient,
		extrinsicClient:            extrinsicClient,
		eventClient:                eventClient,
		metadataClient:             metadataClient,
		lastProcessedBlock:         lastBlock,
		lastProcessedExtrinsicChan: lastExtrinsicChan,
		metaDecode:                 &scalecodec.MetadataDecoder{},
	}
}

func (orchestrator *Orchestrator) Run() {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		ORCHESTRATOR_START,
	).ConsoleLog()
	defer orchestrator.metadataClient.SetIndexerHealthy(false)

	go orchestrator.runExtrinsics()
	go orchestrator.runEvents()

	c := make(chan struct{})
	<-c
}

func (orchestrator *Orchestrator) runExtrinsics() {
	workerName := "EXTRINSIC"

	extrinsicBatchChannel := orchestrator.extrinsicClient.StartBatch()
	startingBlock := orchestrator.extrinsicClient.RecoverLastInsertedBlock()
	lastBlock := orchestrator.rdbClient.GetLastBlockSynced()

	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		ORCHESTRATOR_START_PROCESSING,
		workerName,
		startingBlock,
	).ConsoleLog()

	// atomic.StoreUint64(&orchestrator.extrinsicHeight, uint64(startingBlock))
	orchestrator.lastProcessedExtrinsicChan <- startingBlock

	for {
		for blockHeight := startingBlock + 1; blockHeight <= lastBlock; blockHeight++ {
			lookupKey := orchestrator.rdbClient.GetLookupKeyForBlockHeight(blockHeight)
			extrinsicBatchChannel.SendWork(blockHeight, lookupKey)

			if blockHeight%orchestrator.configuration.ClientsConfig.Extrinsics.BatchSize == 0 {
				extrinsicBatchChannel.Close()
				orchestrator.extrinsicClient.WaitForBatchDbInsertion()
				// atomic.StoreUint64(&orchestrator.extrinsicHeight, uint64(blockHeight))
				orchestrator.lastProcessedExtrinsicChan <- blockHeight
				extrinsicBatchChannel = orchestrator.extrinsicClient.StartBatch()

				orchestrator.metadataClient.UpdateRowCountEstimates()

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
			// atomic.StoreUint64(&orchestrator.extrinsicHeight, uint64(lastBlock))
			orchestrator.lastProcessedExtrinsicChan <- lastBlock
			extrinsicBatchChannel = orchestrator.extrinsicClient.StartBatch()

			orchestrator.metadataClient.UpdateRowCountEstimates()

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
		lastBlock = orchestrator.getLastSyncedBlock()
	}
}

func (orchestrator *Orchestrator) runEvents() {
	workerName := "EVENT"

	go func() {
		for lastExtrinsic := range orchestrator.lastProcessedExtrinsicChan {
			atomic.StoreUint64(&orchestrator.extrinsicHeight, uint64(lastExtrinsic))
		}
	}()

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

	currentSpecVersion := orchestrator.specversionClient.GetSpecVersionAndMetadata(lastProcessedEvent)
	orchestrator.specversionClient.GetMetadata(lastProcessedEvent)

	for {
		lastExtrinsicBlockHeight = int(atomic.LoadUint64(&orchestrator.extrinsicHeight))
		if lastExtrinsicBlockHeight <= lastProcessedEvent {
			continue
		}

		newSpecVersion := orchestrator.specversionClient.GetSpecVersionAndMetadata(lastProcessedEvent + 1)
		if newSpecVersion.SpecVersion != currentSpecVersion.SpecVersion {
			currentSpecVersion = newSpecVersion
			orchestrator.specversionClient.GetMetadata(lastProcessedEvent)
		}

		last := 0
		if currentSpecVersion.Last < lastExtrinsicBlockHeight {
			last = currentSpecVersion.Last
		} else {
			last = lastExtrinsicBlockHeight
		}

		for blockHeight := lastProcessedEvent + 1; blockHeight <= last; blockHeight++ {
			lookupKey := orchestrator.rdbClient.GetLookupKeyForBlockHeight(blockHeight)
			eventBatchChannel.SendWork(blockHeight, lookupKey)

			if blockHeight%orchestrator.configuration.ClientsConfig.Events.BatchSize == 0 {
				eventBatchChannel.Close()
				orchestrator.eventClient.WaitForBatchDbInsertion()
				eventBatchChannel = orchestrator.eventClient.StartBatch()

				orchestrator.metadataClient.UpdateLastProcessedHeight(blockHeight)
				orchestrator.metadataClient.UpdateRowCountEstimates()

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

			orchestrator.metadataClient.UpdateLastProcessedHeight(lastExtrinsicBlockHeight)
			orchestrator.metadataClient.UpdateRowCountEstimates()

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

			orchestrator.metadataClient.UpdateTargetHeight(lastBlock)

			return lastBlock
		}
	}
}
