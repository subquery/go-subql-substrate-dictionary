package extrinsic

import (
	"fmt"
	"go-dictionary/internal/clients/metadata"
	"go-dictionary/internal/clients/specversion"
	"go-dictionary/internal/db/postgres"
	"go-dictionary/internal/db/rocksdb"
	"go-dictionary/internal/messages"
	"strings"

	scale "github.com/itering/scale.go"
	"github.com/itering/scale.go/types"
	"github.com/itering/substrate-api-rpc/util"
)

type (
	ExtrinsicClient struct {
		pgClient               extrinsicRepoClient
		rocksdbClient          *rocksdb.RockClient
		workersCount           int
		batchChan              chan chan *ExtrinsicJob
		specVersions           specversion.SpecVersionRangeList
		specVersionMetadataMap map[string]*metadata.DictionaryMetadata
	}

	extrinsicRepoClient struct {
		*postgres.PostgresClient
		dbChan            chan *Extrinsic
		workersCount      int
		batchFinishedChan chan struct{}
	}

	ExtrinsicBatchChannel struct {
		batchChannel chan *ExtrinsicJob
	}
)

const (
	bodyTypeString = "Vec<Bytes>"
)

func NewExtrinsicClient(
	pgClient *postgres.PostgresClient,
	rocksdbClient *rocksdb.RockClient,
	workersCount int,
	specVersions specversion.SpecVersionRangeList,
	specVersionMetadataMap map[string]*metadata.DictionaryMetadata,
) *ExtrinsicClient {

	batchChan := make(chan chan *ExtrinsicJob, workersCount)
	dbChan := make(chan *Extrinsic, workersCount)
	batchFinishedChan := make(chan struct{}, 1)

	return &ExtrinsicClient{
		pgClient: extrinsicRepoClient{
			pgClient,
			dbChan,
			workersCount,
			batchFinishedChan,
		},
		rocksdbClient:          rocksdbClient,
		workersCount:           workersCount,
		batchChan:              batchChan,
		specVersions:           specVersions,
		specVersionMetadataMap: specVersionMetadataMap,
	}
}

// Run starts the extrinsics worker
func (client *ExtrinsicClient) Run() {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		messages.EXTRINSIC_CLIENT_STARTING,
		client.workersCount,
	).ConsoleLog()

	go client.pgClient.startDbWorker()

	count := 0
	for count < client.workersCount {
		go client.startWorker()
		count++
	}
}

// StartBatch starts the work for a batch of blocks; it creates a channel for the batch
// and sends it to each worker
func (client *ExtrinsicClient) StartBatch() *ExtrinsicBatchChannel {
	// create new job channel for a range
	batchChan := make(chan *ExtrinsicJob, client.workersCount)
	extrinsicBatchChannel := ExtrinsicBatchChannel{
		batchChannel: batchChan,
	}

	// send the job channel to all workers
	for i := 0; i < client.workersCount; i++ {
		client.batchChan <- batchChan
	}

	return &extrinsicBatchChannel
}

// WaitForBatch blocks, waiting for the current batch to finish
func (client *ExtrinsicClient) WaitForBatchDbInsertion() {
	<-client.pgClient.batchFinishedChan
}

// RecoverLastInsertedBlock recovers the last inserted block in db
func (client *ExtrinsicClient) RecoverLastInsertedBlock() int {
	lastBlock := client.pgClient.recoverLastBlock()
	if lastBlock < 0 {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_INFO,
			"",
			nil,
			messages.EXTRINSICS_NO_PREVIOUS_WORK,
		).ConsoleLog()
		return 1 //TODO: use first block from config file
	}

	return lastBlock + 1
}

func (extrinsicBatchChan *ExtrinsicBatchChannel) Close() {
	close(extrinsicBatchChan.batchChannel)
}

// SendWork will send a job to the extrinsic workers
func (extrinsicBatchChan *ExtrinsicBatchChannel) SendWork(blockHeight int, lookupKey []byte) {
	extrinsicBatchChan.batchChannel <- &ExtrinsicJob{
		BlockHeight:    blockHeight,
		BlockLookupKey: lookupKey,
	}
}

func (client *ExtrinsicClient) startWorker() {
	bodyDecoder := types.ScaleDecoder{}
	extrinsicDecoder := scale.ExtrinsicDecoder{}
	extrinsicDecoderOption := types.ScaleDecoderOption{Metadata: nil, Spec: -1}

	for jobChan := range client.batchChan {
		for job := range jobChan {

			rawBodyData, msg := client.rocksdbClient.GetBodyForBlockLookupKey(job.BlockLookupKey)
			if msg != nil {
				msg.ConsoleLog()
				panic(nil)
			}

			bodyDecoder.Init(types.ScaleBytes{Data: rawBodyData}, nil)
			decodedBody := bodyDecoder.ProcessAndUpdateData(bodyTypeString)

			bodyList, ok := decodedBody.([]interface{})
			if !ok {
				messages.NewDictionaryMessage(
					messages.LOG_LEVEL_ERROR,
					messages.GetComponent(client.startWorker),
					nil,
					messages.FAILED_TYPE_ASSERTION,
				).ConsoleLog()
				panic(nil)
			}

			specVersion := client.specVersions.GetSpecVersionForBlock(job.BlockHeight)
			metadata := client.specVersionMetadataMap[fmt.Sprintf("%d", specVersion)].Meta

			if extrinsicDecoderOption.Spec == -1 || extrinsicDecoderOption.Spec != specVersion {
				extrinsicDecoderOption.Metadata = metadata
				extrinsicDecoderOption.Spec = specVersion
			}

			for idx, bodyInterface := range bodyList {
				rawExtrinsic, ok := bodyInterface.(string)
				if !ok {
					messages.NewDictionaryMessage(
						messages.LOG_LEVEL_ERROR,
						messages.GetComponent(client.startWorker),
						nil,
						messages.FAILED_TYPE_ASSERTION,
					).ConsoleLog()
					panic(nil)
				}

				//TODO: "init" with options only if the spec version is new
				extrinsicDecoder.Init(types.ScaleBytes{Data: util.HexToBytes(rawExtrinsic)}, &extrinsicDecoderOption)
				extrinsicDecoder.Process()

				decodedExtrinsic := extrinsicDecoder.Value.(map[string]interface{})
				extrinsicModel := Extrinsic{
					Id:          fmt.Sprintf("%d-%d", job.BlockHeight, idx),
					Module:      getCallModule(job.BlockHeight, decodedExtrinsic),
					Call:        getCallFunction(job.BlockHeight, decodedExtrinsic),
					BlockHeight: job.BlockHeight,
					Success:     true, //TODO: replace with success state from events
					TxHash:      getHash(job.BlockHeight, decodedExtrinsic),
					IsSigned:    isSigned(decodedExtrinsic),
				}
				client.pgClient.insertExtrinsic(&extrinsicModel)
			}
		}
		// send a nil extrinsic when the work is done
		client.pgClient.insertExtrinsic(nil)
	}
}

func getCallModule(blockHeight int, decodedExtrinsic map[string]interface{}) string {
	callModule, ok := decodedExtrinsic[extrinsicCallModuleField].(string)
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getCallModule),
			nil,
			messages.EXTRINSIC_FIELD_FAILED,
			extrinsicCallModuleField,
			blockHeight,
		).ConsoleLog()
		panic(nil)
	}

	return strings.ToLower(callModule)
}

func getCallFunction(blockHeight int, decodedExtrinsic map[string]interface{}) string {
	callFunction, ok := decodedExtrinsic[extrinsicFunctionField].(string)
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getCallFunction),
			nil,
			messages.EXTRINSIC_FIELD_FAILED,
			extrinsicFunctionField,
			blockHeight,
		).ConsoleLog()
		panic(nil)
	}
	return callFunction
}

func getHash(blockHeight int, decodedExtrinsic map[string]interface{}) string {
	txHash, ok := decodedExtrinsic[extrinsicHashField].(string)
	if !ok {
		return ""
		// messages.NewDictionaryMessage(
		// 	messages.LOG_LEVEL_ERROR,
		// 	messages.GetComponent(getHash),
		// 	nil,
		// 	messages.EXTRINSIC_FIELD_FAILED,
		// 	extrinsicHashField,
		// 	blockHeight,
		// ).ConsoleLog()
		// panic(nil)
	}
	return txHash
}

func isSigned(decodedExtrinsic map[string]interface{}) bool {
	_, ok := decodedExtrinsic["signature"]
	return ok
}
