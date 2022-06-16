package event

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"go-dictionary/internal/clients/specversion"
	"go-dictionary/internal/db/postgres"
	"go-dictionary/internal/db/rocksdb"
	"go-dictionary/internal/messages"
	"go-dictionary/models"
	"strconv"
	"strings"

	trieNode "go-dictionary/internal/trie/node"

	scale "github.com/itering/scale.go"

	"github.com/itering/scale.go/types"
)

type (
	EventClient struct {
		pgClient          eventRepoClient
		rocksdbClient     *rocksdb.RockClient
		workersCount      int
		batchChan         chan chan *eventJob
		specVersionClient *specversion.SpecVersionClient
	}

	eventRepoClient struct {
		*postgres.PostgresClient
		dbChan            chan interface{}
		workersCount      int
		batchFinishedChan chan struct{}
	}

	EventBatchChannel struct {
		batchChannel chan *eventJob
	}
)

func NewEventClient(
	pgClient *postgres.PostgresClient,
	rocksdbClient *rocksdb.RockClient,
	workersCount int,
	specVersionClient *specversion.SpecVersionClient,
) *EventClient {
	batchChan := make(chan chan *eventJob, workersCount)
	dbChan := make(chan interface{}, workersCount)
	batchFinishedChan := make(chan struct{}, 1)

	return &EventClient{
		pgClient: eventRepoClient{
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

func (client *EventClient) Run() {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		EVENT_CLIENT_STARTING,
		client.workersCount,
	).ConsoleLog()

	go client.pgClient.startDbWorker()

	for i := 0; i < client.workersCount; i++ {
		go client.startWorker()
	}
}

func (client *EventClient) StartBatch() *EventBatchChannel {
	batchChan := make(chan *eventJob, client.workersCount)
	eventBatchChannel := EventBatchChannel{
		batchChannel: batchChan,
	}

	// send job channel to all workers
	for i := 0; i < client.workersCount; i++ {
		client.batchChan <- batchChan
	}

	return &eventBatchChannel
}

func (client *EventClient) WaitForBatchDbInsertion() {
	<-client.pgClient.batchFinishedChan
}

func (client *EventClient) RecoverLastInsertedBlock() int {
	lastBlock := client.pgClient.recoverLastBlock()
	if lastBlock < 0 {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_INFO,
			"",
			nil,
			EVENT_NO_PREVIOUS_WORK,
		).ConsoleLog()
		return 1 //return first chain block
	}
	return lastBlock
}

func (eventBatchChan *EventBatchChannel) Close() {
	close(eventBatchChan.batchChannel)
}

// SendWork will send a job to the event workers
func (eventBatchChan *EventBatchChannel) SendWork(blockHeight int, lookupKey []byte) {
	eventBatchChan.batchChannel <- &eventJob{
		BlockHeight:    blockHeight,
		BlockLookupKey: lookupKey,
	}
}

func (client *EventClient) startWorker() {
	headerDecoder := types.ScaleDecoder{}
	eventDecoder := scale.EventsDecoder{}
	eventDecoderOption := types.ScaleDecoderOption{Metadata: nil, Spec: -1}

	for jobChan := range client.batchChan {
		for job := range jobChan {
			rawHeaderData := client.rocksdbClient.GetHeaderForBlockLookupKey(job.BlockLookupKey)
			headerDecoder.Init(types.ScaleBytes{Data: rawHeaderData}, nil)
			decodedHeader := headerDecoder.ProcessAndUpdateData(headerTypeString)
			stateRootKey := getStateRootFromRawHeader(decodedHeader)
			rawEvents := client.readRawEvent(stateRootKey)

			specVersionMeta := client.specVersionClient.GetSpecVersionAndMetadata(job.BlockHeight)
			specVersion, _ := strconv.Atoi(specVersionMeta.SpecVersion)
			if eventDecoderOption.Spec != specVersion {
				eventDecoderOption.Metadata = specVersionMeta.Meta
				eventDecoderOption.Spec = specVersion
			}

			eventDecoder.Init(types.ScaleBytes{Data: rawEvents}, &eventDecoderOption)
			eventDecoder.Process()

			eventsCounter := 0
			eventsArray := eventDecoder.Value.([]interface{})
			for _, evt := range eventsArray {
				evtValue := evt.(map[string]interface{})
				eventCall := getEventCall(job.BlockHeight, evtValue)
				eventType := getEventType(job.BlockHeight, evtValue)

				switch eventType {
				// ignore extrinsic success as all extrinsics status was initiated with "true"
				case extrinsicSuccessType:
					{

					}
				case extrinsicFailedType:
					extrinsicUpdate := Event{
						Id:          getEventExtrinsicId(job.BlockHeight, evtValue),
						Event:       eventCall,
						BlockHeight: updateExtrinsicCommand,
					}
					client.pgClient.insertEvent(&extrinsicUpdate)
				case evmLogType:
					eventModel := Event{
						Id:          fmt.Sprintf("%d-%d", job.BlockHeight, eventsCounter),
						Module:      getEventModule(job.BlockHeight, evtValue),
						Event:       getEventCall(job.BlockHeight, evtValue),
						BlockHeight: job.BlockHeight,
					}
					client.pgClient.insertEvent(&eventModel)

					eventParams := getEvmLogParams(job.BlockHeight, evtValue)
					evmLog := EvmLog{
						Id:          fmt.Sprintf("%d-%d", job.BlockHeight, eventsCounter),
						Address:     getEvmLogAddress(job.BlockHeight, eventParams),
						BlockHeight: job.BlockHeight,
						Topics:      getEvmLogTopics(job.BlockHeight, eventParams),
					}
					client.pgClient.insertEvent(&evmLog)

					eventsCounter++
				case ethereumExecutedType:
					eventModel := Event{
						Id:          fmt.Sprintf("%d-%d", job.BlockHeight, eventsCounter),
						Module:      getEventModule(job.BlockHeight, evtValue),
						Event:       getEventCall(job.BlockHeight, evtValue),
						BlockHeight: job.BlockHeight,
					}
					client.pgClient.insertEvent(&eventModel)
					eventsCounter++

					evmTransactionParams := getEvmTransactionParams(job.BlockHeight, evtValue)
					evmTransaction := models.EvmTransaction{
						Id:      fmt.Sprintf("%d-%d", job.BlockHeight, evtValue[extrinsicIdField]),
						TxHash:  getEvmTransactionTxHash(job.BlockHeight, evmTransactionParams),
						From:    getEvmTransactionFromHash(job.BlockHeight, evmTransactionParams),
						To:      getEvmTransactionToHash(job.BlockHeight, evmTransactionParams),
						Success: getEvmTransactionStatus(job.BlockHeight, evmTransactionParams),
					}
					client.pgClient.insertEvent(&evmTransaction)
				default:
					eventModel := Event{
						Id:          fmt.Sprintf("%d-%d", job.BlockHeight, eventsCounter),
						Module:      getEventModule(job.BlockHeight, evtValue),
						Event:       getEventCall(job.BlockHeight, evtValue),
						BlockHeight: job.BlockHeight,
					}
					client.pgClient.insertEvent(&eventModel)
					eventsCounter++
				}
			}
		}
		// send nil event when work is done
		client.pgClient.insertEvent(nil)
	}
}

// readRawEvent reads a raw event from rocksdb and returns it
func (client *EventClient) readRawEvent(rootStateKey string) []byte {
	var err error
	stateKey := make([]byte, 64)

	stateKey, err = hex.DecodeString(rootStateKey)
	if err != nil {
		panic(err)
	}

	nibbleCount := 0
	for nibbleCount != triePathNibbleCount {
		node := client.rocksdbClient.GetStateTrieNode(stateKey)
		decodedNode, err := trieNode.Decode(bytes.NewReader(node))
		if err != nil {
			panic(err)
		}

		switch decodedNode.Type() {
		case trieNode.BranchType:
			{
				decodedBranch := decodedNode.(*trieNode.Branch)

				// jump over the partial key
				nibbleCount += len(decodedBranch.Key)
				if nibbleCount == triePathNibbleCount {
					return decodedBranch.Value
				}

				childHash := decodedBranch.Children[eventTriePathHexNibbles[nibbleCount]].GetHash()
				nibbleCount++

				stateKey = append([]byte{}, eventTriePathBytes[:nibbleCount/2]...)
				if nibbleCount%2 == 1 {
					stateKey = append(stateKey, nibbleToZeroPaddedByte[eventTriePathHexNibbles[nibbleCount-1]])
				}
				stateKey = append(stateKey, childHash...)
			}
		case trieNode.LeafType:
			{
				decodedLeaf := decodedNode.(*trieNode.Leaf)
				return decodedLeaf.Value
			}
		}
	}
	return nil
}

// getStateRootFromRawHeader gets the state root from a decoded block header
func getStateRootFromRawHeader(rawHeader interface{}) string {
	stateRoot, ok := rawHeader.(map[string]interface{})["state_root"].(string)
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getStateRootFromRawHeader),
			nil,
			EVENT_STATE_ROOT_NOT_FOUND,
		).ConsoleLog()
	}

	return strings.TrimPrefix(stateRoot, "0x")
}

func getEventId(blockHeight int, decodedEvent map[string]interface{}) string {
	eventId, ok := decodedEvent[eventIdField].(int)
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getEventId),
			nil,
			EVENT_FIELD_FAILED,
			eventIdField,
			blockHeight,
		).ConsoleLog()
	}

	return fmt.Sprintf("%d-%d", blockHeight, eventId)
}

func getEventModule(blockHeight int, decodedEvent map[string]interface{}) string {
	module, ok := decodedEvent[eventModuleField].(string)
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getEventModule),
			nil,
			EVENT_FIELD_FAILED,
			eventModuleField,
			blockHeight,
		).ConsoleLog()
	}
	return strings.ToLower(module)
}

func getEventCall(blockHeight int, decodedEvent map[string]interface{}) string {
	event, ok := decodedEvent[eventEventField].(string)
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getEventCall),
			nil,
			EVENT_FIELD_FAILED,
			eventEventField,
			blockHeight,
		).ConsoleLog()
	}
	return event
}

func getEventType(blockHeight int, decodedEvent map[string]interface{}) string {
	eventType, ok := decodedEvent[eventTypeField].(string)
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getEventType),
			nil,
			EVENT_FIELD_FAILED,
			eventTypeField,
			blockHeight,
		).ConsoleLog()
	}

	return eventType
}

func getEventExtrinsicId(blockHeight int, decodedEvent map[string]interface{}) string {
	extrinsicId, ok := decodedEvent[extrinsicIdField].(int)
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getEventExtrinsicId),
			nil,
			EVENT_FIELD_FAILED,
			extrinsicIdField,
			blockHeight,
		).ConsoleLog()
	}

	return fmt.Sprintf("%d-%d", blockHeight, extrinsicId)
}

func getEvmLogParams(blockHeight int, decodedEvent map[string]interface{}) map[string]interface{} {
	params, ok := decodedEvent[eventParams].([]scale.EventParam)
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getEvmLogParams),
			nil,
			EVENT_FIELD_FAILED,
			eventParams,
			blockHeight,
		).ConsoleLog()
	}
	return params[0].Value.(map[string]interface{})
}

func getEvmLogAddress(blockHeight int, params map[string]interface{}) string {
	address, ok := params[eventParamsAddress].(string)
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getEvmLogAddress),
			nil,
			EVENT_FIELD_FAILED,
			eventParamsAddress,
			blockHeight,
		).ConsoleLog()
	}
	return address
}

func getEvmLogTopics(blockHeight int, params map[string]interface{}) []string {
	topics, ok := params[eventParamsTopics].([]interface{})
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getEvmLogTopics),
			nil,
			EVENT_FIELD_FAILED,
			eventParamsTopics,
			blockHeight,
		).ConsoleLog()
	}

	stringTopics := make([]string, topicsLen)
	for i, topic := range topics {
		stringTopics[i] = topic.(string)
	}
	return stringTopics
}

func getEvmTransactionParams(blockHeight int, decodedEvent map[string]interface{}) []scale.EventParam {
	params, ok := decodedEvent[eventParams].([]scale.EventParam)
	// from, to/contract_address, transaction_hash, exit_reason
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getEvmTransactionParams),
			nil,
			EVENT_FIELD_FAILED,
			eventParams,
			blockHeight,
		).ConsoleLog()
	}
	return params
}

func getEvmTransactionFromHash(blockHeight int, params []scale.EventParam) string {
	fromHash, ok := params[0].Value.(string)
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getEvmTransactionFromHash),
			nil,
			EVENT_FIELD_FAILED,
			"fromHash",
			blockHeight,
		).ConsoleLog()
	}
	return fromHash
}

func getEvmTransactionToHash(blockHeight int, params []scale.EventParam) string {
	toHash, ok := params[1].Value.(string)
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getEvmTransactionToHash),
			nil,
			EVENT_FIELD_FAILED,
			"toHash",
			blockHeight,
		).ConsoleLog()
	}
	return toHash
}

func getEvmTransactionTxHash(blockHeight int, params []scale.EventParam) string {
	txHash, ok := params[2].Value.(string)
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getEvmTransactionTxHash),
			nil,
			EVENT_FIELD_FAILED,
			"txHash",
			blockHeight,
		).ConsoleLog()
	}
	return txHash
}

func getEvmTransactionStatus(blockHeight int, params []scale.EventParam) bool {
	evmTransactionStatus, ok := params[3].Value.(map[string]interface{})
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getEvmTransactionStatus),
			nil,
			EVENT_FIELD_FAILED,
			"evmTransactionStatus",
			blockHeight,
		).ConsoleLog()
	}
	for evmTransactionKey := range evmTransactionStatus {
		if evmTransactionKey == "Succeed" {
			return true
		}
	}
	return false
}
