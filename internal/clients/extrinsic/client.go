package extrinsic

import (
	"fmt"
	"go-dictionary/internal/clients/metadata"
	"go-dictionary/internal/clients/specversion"
	"go-dictionary/internal/db/postgres"
	"go-dictionary/internal/db/rocksdb"
	"go-dictionary/internal/messages"
	"strings"

	"github.com/itering/scale.go/types"
	"github.com/itering/substrate-api-rpc"
)

type (
	ExtrinsicClient struct {
		pgClient               extrinsicRepoClient
		rocksdbClient          *rocksdb.RockClient
		workersCount           int
		jobChan                chan *ExtrinsicJob
		specVersions           specversion.SpecVersionRangeList
		specVersionMetadataMap map[string]*metadata.DictionaryMetadata
	}

	extrinsicRepoClient struct {
		*postgres.PostgresClient
		batchSize int
		dbChan    chan *Extrinsic
	}
)

const (
	bodyTypeString = "Vec<Bytes>"
)

func NewExtrinsicClient(
	pgClient *postgres.PostgresClient,
	dbBatchSize int,
	rocksdbClient *rocksdb.RockClient,
	workersCount int,
	specVersions specversion.SpecVersionRangeList,
	specVersionMetadataMap map[string]*metadata.DictionaryMetadata,
) *ExtrinsicClient {
	jobChan := make(chan *ExtrinsicJob, workersCount)
	dbChan := make(chan *Extrinsic, workersCount)
	return &ExtrinsicClient{
		pgClient: extrinsicRepoClient{
			pgClient,
			dbBatchSize,
			dbChan,
		},
		rocksdbClient:          rocksdbClient,
		workersCount:           workersCount,
		jobChan:                jobChan,
		specVersions:           specVersions,
		specVersionMetadataMap: specVersionMetadataMap,
	}
}

func (client *ExtrinsicClient) Run() *messages.DictionaryMessage {
	//start db worker
	//start extrinsic workers
	return nil
}

// SendWork will send a job to the extrinsic workers
func (client *ExtrinsicClient) SendWork(blockHeight int, lookupKey []byte) {
	client.jobChan <- &ExtrinsicJob{
		BlockHeight:    blockHeight,
		BlockLookupKey: lookupKey,
	}
}

func (client *ExtrinsicClient) startWorker() {
	bodyDecoder := types.ScaleDecoder{}

	for {
		job := <-client.jobChan

		rawBodyData, msg := client.rocksdbClient.GetBodyForBlockLookupKey(job.BlockLookupKey)
		if msg != nil {
			msg.ConsoleLog()
			panic(nil) // we can execute deffered code
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

		rawExtrinsicList := []string{}
		for _, bodyInterface := range bodyList {
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
			rawExtrinsicList = append(rawExtrinsicList, rawExtrinsic)
		}

		specVersion := client.specVersions.GetSpecVersionForBlock(job.BlockHeight)
		metadataInstant := client.specVersionMetadataMap[fmt.Sprintf("%d", specVersion)].MetaInstant

		decodedExtrinsics, err := substrate.DecodeExtrinsic(rawExtrinsicList, metadataInstant, specVersion)
		if err != nil {
			messages.NewDictionaryMessage(
				messages.LOG_LEVEL_ERROR,
				messages.GetComponent(client.startWorker),
				err,
				messages.EXTRINSIC_DECODE_FAILED,
				job.BlockHeight,
			).ConsoleLog()
			panic(nil)
		}

		for idx, decodedExtrinsic := range decodedExtrinsics {
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
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getHash()),
			nil,
			messages.EXTRINSIC_FIELD_FAILED,
			extrinsicHashField,
			blockHeight,
		).ConsoleLog()
		panic(nil)
	}
	return txHash
}

func isSigned(decodedExtrinsic map[string]interface{}) bool {
	_, ok := decodedExtrinsic["signature"]
	return ok
}
