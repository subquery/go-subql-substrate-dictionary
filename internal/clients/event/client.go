package event

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"go-dictionary/internal/clients/metadata"
	"go-dictionary/internal/clients/specversion"
	"go-dictionary/internal/db/postgres"
	"go-dictionary/internal/db/rocksdb"
	"go-dictionary/internal/messages"
	"strconv"
	"strings"

	trieNode "go-dictionary/internal/trie/node"

	scalecodec "github.com/itering/scale.go"

	"github.com/itering/scale.go/types"
)

type (
	EventClient struct {
		pgClient               eventRepoClient
		rocksdbClient          *rocksdb.RockClient
		workersCount           int
		batchChan              chan chan *eventJob
		specVersions           specversion.SpecVersionRangeList
		specVersionMetadataMap map[string]*metadata.DictionaryMetadata
	}

	eventRepoClient struct {
		*postgres.PostgresClient
		dbChan            chan *Event
		workersCount      int
		batchFinishedChan chan struct{}
	}

	EventBatchChannel struct {
		batchChannel chan *eventJob
	}
)

var (
	hexNibbleToByte map[rune]byte = map[rune]byte{
		'0': 0,
		'1': 1,
		'2': 2,
		'3': 3,
		'4': 4,
		'5': 5,
		'6': 6,
		'7': 7,
		'8': 8,
		'9': 9,
		'a': 0xa,
		'b': 0xb,
		'c': 0xc,
		'd': 0xd,
		'e': 0xe,
		'f': 0xf,
	}
)

func NewEventClient(
	pgClient *postgres.PostgresClient,
	rocksdbClient *rocksdb.RockClient,
	workersCount int,
	specVersions specversion.SpecVersionRangeList,
	specVersionMetadataMap map[string]*metadata.DictionaryMetadata,
) *EventClient {
	batchChan := make(chan chan *eventJob, workersCount)
	dbChan := make(chan *Event, workersCount)
	batchFinishedChan := make(chan struct{}, 1)

	return &EventClient{
		pgClient: eventRepoClient{
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

func (client *EventClient) Run() {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		messages.EVENT_CLIENT_STARTING,
	).ConsoleLog()

	//TODO: start db client

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
	//TODO: implement
	return -1
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
	eventDecoder := scalecodec.EventsDecoder{}
	eventDecoderOption := types.ScaleDecoderOption{Metadata: nil, Spec: -1}

	for jobChan := range client.batchChan {
		for job := range jobChan {

			rawHeaderData, msg := client.rocksdbClient.GetHeaderForBlockLookupKey(job.BlockLookupKey)
			if msg != nil {
				msg.ConsoleLog()
				panic(nil)
			}

			headerDecoder.Init(types.ScaleBytes{Data: rawHeaderData}, nil)
			decodedHeader := headerDecoder.ProcessAndUpdateData(headerTypeString)
			stateRootKey := getStateRootFromRawHeader(decodedHeader)
			rawEvents := client.readRawEvent(stateRootKey)

			specVersion := client.specVersions.GetSpecVersionForBlock(job.BlockHeight)
			metadata := client.specVersionMetadataMap[fmt.Sprintf("%d", specVersion)].Meta

			if eventDecoderOption.Spec == -1 || eventDecoderOption.Spec != specVersion {
				eventDecoderOption.Metadata = metadata
				eventDecoderOption.Spec = specVersion
			}

			eventDecoder.Init(types.ScaleBytes{Data: rawEvents}, &eventDecoderOption)
			eventDecoder.Process()

			eventsArray := eventDecoder.Value.([]interface{})
			for _, evt := range eventsArray {
				fmt.Println("id:", evt.(map[string]interface{})["event_idx"].(int))
				fmt.Println("module:", strings.ToLower(evt.(map[string]interface{})["module_id"].(string)))
				fmt.Println("event:", evt.(map[string]interface{})["event_id"].(string))
			}
		}
	}
}

// readRawEvent reads a raw event from rocksdb and returns it
func (client *EventClient) readRawEvent(rootStateKey string) []byte {
	stateKey, err := hex.DecodeString(rootStateKey)
	if err != nil {
		panic(err)
	}

	eventsPathKey := eventTriePathKey
	prefix := []byte{}
	nibblePos := 0

	for len(eventsPathKey) != 0 {
		node, msg := client.rocksdbClient.GetStateTrieNode(stateKey)
		if msg != nil {
			msg.ConsoleLog()
			panic(nil)
		}

		decodedNode, err := trieNode.Decode(bytes.NewReader(node))
		if err != nil {
			panic(err)
		}

		switch decodedNode.Type() {
		case trieNode.BranchType:
			{
				decodedBranch := decodedNode.(*trieNode.Branch)

				key := decodedBranch.GetKey()
				if len(key) != 0 {
					keyNibblePos := 0
					keyBytes := []byte{}
					for _, keyNibble := range key {
						prefix = insertNibble(prefix, nibblePos, keyNibble)
						keyBytes = insertNibble(keyBytes, keyNibblePos, keyNibble)
						nibblePos++
						keyNibblePos++
					}
					eventsPathKey = strings.TrimPrefix(eventsPathKey, hex.EncodeToString(keyBytes))
				}

				if len(eventsPathKey) == 0 {
					return decodedBranch.GetValue()
				}

				childIndex := []byte{eventsPathKey[0]}
				index, err := strconv.ParseInt(string(childIndex), 16, 64)
				if err != nil {
					panic(err)
				}
				prefix = insertNibble(prefix, nibblePos, hexNibbleToByte[rune(eventsPathKey[0])])
				nibblePos++

				childHash := decodedBranch.Children[index].GetHash()
				stateKey = append(prefix, childHash...)

				eventsPathKey = eventsPathKey[1:]
			}
		case trieNode.LeafType:
			{
				decodedLeaf := decodedNode.(*trieNode.Leaf)
				return decodedLeaf.GetValue()
			}
		}
	}
	return nil
}

func insertNibble(dest []byte, nibblePos int, nibble byte) []byte {
	if nibblePos%2 == 0 {
		dest = append(dest, nibble<<4&0xf0)
		return dest
	}
	dest[len(dest)-1] = dest[len(dest)-1] | nibble&0xf
	return dest
}

// getStateRootFromRawHeader gets the state root from a decoded block header
func getStateRootFromRawHeader(rawHeader interface{}) string {
	stateRoot, ok := rawHeader.(map[string]interface{})["state_root"].(string)
	if !ok {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(getStateRootFromRawHeader),
			nil,
			messages.EVENT_STATE_ROOT_NOT_FOUND,
		).ConsoleLog()
		panic(nil)
	}

	return strings.TrimPrefix(stateRoot, "0x")
}
