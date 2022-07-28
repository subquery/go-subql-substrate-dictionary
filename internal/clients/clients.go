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
	"strconv"

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
		metadataClient     *metadata.MetadataClient
		lastProcessedBlock int
		rowCountEstimate   []metadata.RowCountEstimate
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
	specVersionClient.Run()
	specName := specVersionClient.GetSpecName()

	// EXTRINSIC - extrinsic client
	extrinsicClient := extrinsic.NewExtrinsicClient(
		pgClient,
		rdbClient,
		config.ClientsConfig.ExtrinsicsWorkers,
		specVersionClient,
	)
	extrinsicClient.Run(config.Evm)
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		ORCHESTRATOR_BATCH_SIZE,
		"Extrinsic",
		config.ClientsConfig.BatchSize,
	).ConsoleLog()

	// EVENTS - event client
	eventClient := event.NewEventClient(
		pgClient,
		rdbClient,
		config.ClientsConfig.EventsWorkers,
		specVersionClient,
		config.IssueBlocks,
	)
	eventClient.Run(config.Evm)
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		ORCHESTRATOR_BATCH_SIZE,
		"Event",
		config.ClientsConfig.BatchSize,
	).ConsoleLog()

	// METADATA - metadata client
	metadataClient := metadata.NewMetadataClient(
		pgClient,
		rdbClient,
		config.ChainConfig.HttpRpcEndpoint,
		config.PostgresConfig.Schema,
	)
	metadataClient.Run(specName)
	metadataClient.SetIndexerHealthy(true)
	metadataClient.SetIndexerVersion(config.IndexerVersion)
	metadataClient.UpdateTargetHeight(lastBlock)

	return &Orchestrator{
		configuration:      config,
		pgClient:           pgClient,
		rdbClient:          rdbClient,
		specversionClient:  specVersionClient,
		extrinsicClient:    extrinsicClient,
		eventClient:        eventClient,
		metadataClient:     metadataClient,
		lastProcessedBlock: lastBlock,
	}
}

func (orchestrator *Orchestrator) Run() {
	currentSpecv := &specversion.SpecVersionRange{}
	var specvNum int
	workerExtrinsic := "EXTRINSIC"
	workerEvent := "EVENT"

	extrinsicBatchChannel := orchestrator.extrinsicClient.StartBatch()
	startingExtrinsic := orchestrator.extrinsicClient.RecoverLastInsertedBlock()

	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		ORCHESTRATOR_START_PROCESSING,
		workerExtrinsic,
		startingExtrinsic,
	).ConsoleLog()

	eventBatchChannel := orchestrator.eventClient.StartBatch()
	startingEvent := orchestrator.eventClient.RecoverLastInsertedBlock()
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		ORCHESTRATOR_START_PROCESSING,
		workerEvent,
		startingEvent,
	).ConsoleLog()

	lastBlock := orchestrator.rdbClient.GetLastBlockSynced()
	if startingExtrinsic == 0 && lastBlock != 0 {
		currentSpecv = orchestrator.specversionClient.GetSpecVersion(startingExtrinsic + 1)
		orchestrator.specversionClient.UpdateMetadata(startingExtrinsic + 1)
	} else {
		currentSpecv = orchestrator.specversionClient.GetSpecVersion(startingExtrinsic)
		orchestrator.specversionClient.UpdateMetadata(startingExtrinsic)
	}
	specvNum, _ = strconv.Atoi(currentSpecv.SpecVersion)

	// sync events if left behind
	if startingEvent < startingExtrinsic {
		for blockHeight := startingEvent + 1; blockHeight <= startingExtrinsic; blockHeight++ {
			lookupKey := orchestrator.rdbClient.GetLookupKeyForBlockHeight(blockHeight)
			eventBatchChannel.SendWork(blockHeight, lookupKey, specvNum)
		}

		eventBatchChannel.Close()
		orchestrator.eventClient.WaitForBatchDbInsertion()
		eventBatchChannel = orchestrator.eventClient.StartBatch()

		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_SUCCESS,
			"",
			nil,
			ORCHESTRATOR_FINISH_BATCH,
			workerEvent,
			startingExtrinsic,
		).ConsoleLog()
	}

	orchestrator.metadataClient.UpdateLastProcessedHeight(startingExtrinsic)
	orchestrator.rowCountEstimate = orchestrator.metadataClient.SetRowCountEstimates()

	for {
		for lastBlock >= currentSpecv.Last {
			if lastBlock > currentSpecv.Last {
				newSpecv := orchestrator.specversionClient.GetSpecVersion(startingExtrinsic + 1)
				if newSpecv.SpecVersion != currentSpecv.SpecVersion {
					currentSpecv = newSpecv
					orchestrator.specversionClient.UpdateMetadata(startingExtrinsic + 1)
					specvNum, _ = strconv.Atoi(currentSpecv.SpecVersion)
				}
			}

			for blockHeight := startingExtrinsic + 1; blockHeight <= currentSpecv.Last; blockHeight++ {
				lookupKey := orchestrator.rdbClient.GetLookupKeyForBlockHeight(blockHeight)
				extrinsicBatchChannel.SendWork(blockHeight, lookupKey, specvNum)
				eventBatchChannel.SendWork(blockHeight, lookupKey, specvNum)

				if blockHeight%orchestrator.configuration.ClientsConfig.BatchSize == 0 {
					extrinsicBatchChannel, eventBatchChannel = orchestrator.finishBatches(
						extrinsicBatchChannel,
						eventBatchChannel,
						blockHeight,
					)
				}
			}

			if currentSpecv.Last%orchestrator.configuration.ClientsConfig.BatchSize != 0 {
				extrinsicBatchChannel, eventBatchChannel = orchestrator.finishBatches(
					extrinsicBatchChannel,
					eventBatchChannel,
					currentSpecv.Last,
				)
			}

			startingExtrinsic = currentSpecv.Last
			if lastBlock == currentSpecv.Last {
				break
			}
		}

		lastBlock = orchestrator.getLastSyncedBlock()
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
			//TODO: update estimate

			return lastBlock
		}
	}
}

func (orchestrator *Orchestrator) finishBatches(
	extrinsicBatchChannel *extrinsic.ExtrinsicBatchChannel,
	eventBatchChannel *event.EventBatchChannel,
	finishedHeight int,
) (*extrinsic.ExtrinsicBatchChannel, *event.EventBatchChannel) {
	workerOrchestrator := "ORCHESTRATOR"

	extrinsicBatchChannel.Close()
	orchestrator.extrinsicClient.WaitForBatchDbInsertion()
	eventBatchChannel.Close()
	lastInserts := orchestrator.eventClient.WaitForBatchDbInsertion()
	orchestrator.updateTableEstimates(lastInserts)

	extrBatchChan := orchestrator.extrinsicClient.StartBatch()
	evBatchChan := orchestrator.eventClient.StartBatch()

	orchestrator.metadataClient.UpdateLastProcessedHeight(finishedHeight)

	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_SUCCESS,
		"",
		nil,
		ORCHESTRATOR_FINISH_BATCH,
		workerOrchestrator,
		finishedHeight,
	).ConsoleLog()

	return extrBatchChan, evBatchChan
}

// updateTableEstimates updates the local tables estimates and the database record
func (orchestrator *Orchestrator) updateTableEstimates(newInserts map[string]int) {
	for idx, estimate := range orchestrator.rowCountEstimate {
		if insertedNumber, ok := newInserts[estimate.Table]; ok {
			orchestrator.rowCountEstimate[idx].Estimate += insertedNumber
		}
	}
	go orchestrator.metadataClient.UpdateRowCountEstimates(orchestrator.rowCountEstimate)
}
