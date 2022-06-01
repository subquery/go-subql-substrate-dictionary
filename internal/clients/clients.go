package clients

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"go-dictionary/internal/clients/extrinsic"
	"go-dictionary/internal/clients/metadata"
	"go-dictionary/internal/clients/specversion"
	"go-dictionary/internal/config"
	"go-dictionary/internal/db/postgres"
	"go-dictionary/internal/db/rocksdb"
	"go-dictionary/internal/messages"
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	"github.com/itering/scale.go/source"
	"github.com/itering/scale.go/types"

	trieNode "go-dictionary/internal/trie/node"
)

type (
	Orchestrator struct {
		configuration      config.Config
		pgClient           *postgres.PostgresClient
		rdbClient          *rocksdb.RockClient
		lastBlock          int
		specversionClient  *specversion.SpecVersionClient
		specVersionRange   specversion.SpecVersionRangeList
		metadataClient     *metadata.MetadataClient
		specVersionMetaMap map[string]*metadata.DictionaryMetadata
		extrinsicClient    *extrinsic.ExtrinsicClient
	}
)

// Neworchestrator creates and initialises a new orchestrator
func NewOrchestrator(
	config config.Config,
) *Orchestrator {
	// Postgres connect
	pgClient, dictionaryMessage := postgres.Connect(config.PostgresConfig)
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		panic(nil)
	}

	// Rocksdb connect
	rdbClient, dictionaryMessage := rocksdb.OpenRocksdb(
		config.RocksdbConfig.RocksdbPath,
		config.RocksdbConfig.RocksdbSecondaryPath,
	)
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		panic(nil)
	}

	lastBlock, dictionaryMessage := rdbClient.GetLastBlockSynced()
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		panic(nil)
	}
	// SPEC VERSION -- spec version and ranges for each spec
	specVersionClient := specversion.NewSpecVersionClient(
		config.ChainConfig.FirstSpecVersion,
		lastBlock,
		rdbClient,
		pgClient,
		config.ChainConfig.HttpRpcEndpoint,
	)

	specVersionsRange, dictionaryMessage := specVersionClient.Run()
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		panic(nil)
	}

	// METADATA -- meta for spec version
	metadataClient := metadata.NewMetadataClient(
		rdbClient,
		config.ChainConfig.HttpRpcEndpoint,
	)

	specVersionMetadataMap, dictionaryMessage := metadataClient.GetMetadata(specVersionsRange)
	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		panic(nil)
	}

	// Register custom types
	//TODO: file path from config file
	c, err := ioutil.ReadFile("./network/polkadot.json")
	if err != nil {
		log.Println("[ERR] Failed to register types for network Polkadot:", err)
		return nil
	}
	types.RegCustomTypes(source.LoadTypeRegistry(c))

	// EXTRINSIC - extrinsic client
	extrinsicClient := extrinsic.NewExtrinsicClient(
		pgClient,
		rdbClient,
		config.ClientsConfig.Extrinsics.Workers,
		specVersionsRange,
		specVersionMetadataMap,
	)
	extrinsicClient.Run()

	return &Orchestrator{
		configuration:      config,
		pgClient:           pgClient,
		rdbClient:          rdbClient,
		lastBlock:          lastBlock,
		specversionClient:  specVersionClient,
		specVersionRange:   specVersionsRange,
		metadataClient:     metadataClient,
		specVersionMetaMap: specVersionMetadataMap,
		extrinsicClient:    extrinsicClient,
	}
}

func (orchestrator *Orchestrator) Run() {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		messages.ORCHESTRATOR_START,
	).ConsoleLog()

	var batchChannel *extrinsic.ExtrinsicBatchChannel
	batchChannel = orchestrator.extrinsicClient.StartBatch()

	startingBlock := orchestrator.extrinsicClient.RecoverLastInsertedBlock()
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		messages.ORCHESTRATOR_START_EXTRINSIC_BATCH,
		orchestrator.configuration.ClientsConfig.Extrinsics.BatchSize,
		startingBlock,
	).ConsoleLog()

	for blockHeight := startingBlock; blockHeight <= orchestrator.lastBlock; blockHeight++ {
		if blockHeight%orchestrator.configuration.ClientsConfig.Extrinsics.BatchSize == 0 {
			batchChannel.Close()
			orchestrator.extrinsicClient.WaitForBatchDbInsertion()

			messages.NewDictionaryMessage(
				messages.LOG_LEVEL_SUCCESS,
				"",
				nil,
				messages.ORCHESTRATOR_FINISH_EXTRINSIC_BATCH,
			).ConsoleLog()

			batchChannel = orchestrator.extrinsicClient.StartBatch()

			messages.NewDictionaryMessage(
				messages.LOG_LEVEL_INFO,
				"",
				nil,
				messages.ORCHESTRATOR_START_EXTRINSIC_BATCH,
				orchestrator.configuration.ClientsConfig.Extrinsics.BatchSize,
				blockHeight,
			).ConsoleLog()
		}

		lookupKey, msg := orchestrator.rdbClient.GetLookupKeyForBlockHeight(blockHeight)
		if msg != nil {
			msg.ConsoleLog()
			panic(nil)
		}

		batchChannel.SendWork(blockHeight, lookupKey)
	}

	//TODO: show some messages
	batchChannel.Close()
	orchestrator.extrinsicClient.WaitForBatchDbInsertion()
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

func (orchestrator *Orchestrator) ReadEvent() {
	eventsPathKey := "26aa394eea5630e07c48ae0c9558cef780d41e5e16056765bc8461851072c9d7"
	rootKey := "0d08df9961a53461e736dc1973c6c55c114bc6df0f381e51b89e7234b96d3016"
	stateKey, err := hex.DecodeString(rootKey)
	if err != nil {
		panic(err)
		//TODO: better logging
	}

	for len(eventsPathKey) != 0 {
		node, msg := orchestrator.rdbClient.GetStateTrieNode(stateKey)
		if msg != nil {
			msg.ConsoleLog()
			panic(nil)
		}

		decodedNode, err := trieNode.Decode(bytes.NewReader(node))
		if err != nil {
			fmt.Println(err)
			panic(nil)
		}

		switch decodedNode.Type() {
		case trieNode.BranchType:
			{
				prefix := []byte{}

				decodedBranch := decodedNode.(*trieNode.Branch)

				fmt.Println(decodedBranch.ScaleEncodeHash())
				key := decodedBranch.GetKey()
				if len(key) != 0 {
					prefix = append(prefix, key...)
					hexStringKey := hex.EncodeToString(key)
					eventsPathKey = strings.TrimPrefix(eventsPathKey, hexStringKey)
				}

				childIndex := []byte{eventsPathKey[0]}
				index, err := strconv.ParseInt(string(childIndex), 16, 64)
				prefix = append(prefix, byte(index))
				if err != nil {
					panic(err)
				}

				childHash := decodedBranch.Children[index].GetHash()
				stateKey = append(prefix, childHash...)

				fmt.Println(stateKey)
				eventsPathKey = eventsPathKey[1:]
			}
		case trieNode.LeafType:
			{
				eventsPathKey = string("")
				decodedLeaf := decodedNode.(*trieNode.Leaf)
				fmt.Println(decodedLeaf.Value)
			}
		}

	}
}
