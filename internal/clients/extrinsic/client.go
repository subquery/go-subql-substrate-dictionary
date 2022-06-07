package extrinsic

import (
	"fmt"
	"go-dictionary/internal/clients/specversion"
	"go-dictionary/internal/db/postgres"
	"go-dictionary/internal/db/rocksdb"
	"go-dictionary/internal/messages"
	"strconv"
	"strings"

	scale "github.com/itering/scale.go"
	"github.com/itering/scale.go/types"
	"github.com/itering/substrate-api-rpc/util"
)

type (
	ExtrinsicClient struct {
		pgClient          extrinsicRepoClient
		rocksdbClient     *rocksdb.RockClient
		workersCount      int
		batchChan         chan chan *ExtrinsicJob
		specVersionClient *specversion.SpecVersionClient
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
	specVersionClient *specversion.SpecVersionClient,
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
		rocksdbClient:     rocksdbClient,
		workersCount:      workersCount,
		batchChan:         batchChan,
		specVersionClient: specVersionClient,
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
		return 1
	}

	return lastBlock
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
			rawBodyData := client.rocksdbClient.GetBodyForBlockLookupKey(job.BlockLookupKey)
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
			}

			specVersionMeta := client.specVersionClient.GetSpecVersionAndMetadata(job.BlockHeight)
			specVersion, _ := strconv.Atoi(specVersionMeta.SpecVersion)
			if extrinsicDecoderOption.Spec == -1 || extrinsicDecoderOption.Spec != specVersion {
				extrinsicDecoderOption.Metadata = specVersionMeta.Meta
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
					Success:     true, //the real value is get from events
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
	}
	return callFunction
}

func getHash(blockHeight int, decodedExtrinsic map[string]interface{}) string {
	txHash, ok := decodedExtrinsic[extrinsicHashField].(string)
	if !ok {
		//TODO: calculate "hash" if not found
		return ""
	}
	return txHash
}

func isSigned(decodedExtrinsic map[string]interface{}) bool {
	_, ok := decodedExtrinsic["signature"]
	return ok
}
