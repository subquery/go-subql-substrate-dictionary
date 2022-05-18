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
		//TODO: function for each part of extrinsic??
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
			extrinsicModel := Extrinsic{}
			extrinsicModel.Id = fmt.Sprintf("%d-%d", job.BlockHeight, idx)

			callModule, ok := decodedExtrinsic["call_module"].(string)
			if !ok {
				messages.NewDictionaryMessage(
					messages.LOG_LEVEL_ERROR,
					messages.GetComponent(client.startWorker),
					nil,
					messages.EXTRINSIC_FIELD_FAILED,
					"call_module",
					job.BlockHeight,
				).ConsoleLog()
				panic(nil)
			}
			extrinsicModel.Module = strings.ToLower(callModule)

			callFunction, ok := decodedExtrinsic["call_module_function"].(string)
			if !ok {
				messages.NewDictionaryMessage(
					messages.LOG_LEVEL_ERROR,
					messages.GetComponent(client.startWorker),
					nil,
					messages.EXTRINSIC_FIELD_FAILED,
					"call_module_function",
					job.BlockHeight,
				).ConsoleLog()
				panic(nil)
			}
			extrinsicModel.Call = callFunction

			extrinsicModel.BlockHeight = job.BlockHeight
			extrinsicModel.Success = true //TODO: replace with success state from events

			txHash, ok := decodedExtrinsic["extrinsic_hash"].(string)
			if !ok {
				messages.NewDictionaryMessage(
					messages.LOG_LEVEL_ERROR,
					messages.GetComponent(client.startWorker),
					nil,
					messages.EXTRINSIC_FIELD_FAILED,
					"extrinsic_hash",
					job.BlockHeight,
				).ConsoleLog()
				panic(nil)
			}
			extrinsicModel.TxHash = txHash

			_, ok = decodedExtrinsic["signature"]
			if !ok {
				extrinsicModel.IsSigned = false
			} else {
				extrinsicModel.IsSigned = true
			}
		}
	}
}
