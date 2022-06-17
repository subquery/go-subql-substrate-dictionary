package rocksdb

import (
	"encoding/hex"
	"go-dictionary/internal/config"
	"go-dictionary/internal/messages"
	"strconv"

	"github.com/linxGnu/grocksdb"
)

const (
	/// Metadata about chain
	COL_META = iota + 1
	COL_STATE
	COL_STATE_META
	/// maps hashes -> lookup keys and numbers to canon hashes
	COL_KEY_LOOKUP
	/// Part of Block
	COL_HEADER
	COL_BODY
	COL_JUSTIFICATION
	/// Stores the changes tries for querying changed storage of a block
	COL_CHANGES_TRIE
	COL_AUX
	/// Off Chain workers local storage
	COL_OFFCHAIN
	COL_CACHE
	COL_TRANSACTION
)

type RockClient struct {
	db            *grocksdb.DB
	columnHandles []*grocksdb.ColumnFamilyHandle
	opts          *grocksdb.Options
	ro            *grocksdb.ReadOptions
}

// OpenRocksdb connect to the rocksdb instance indicated by a path argument
func OpenRocksdb(config config.RocksdbConfig) *RockClient {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		messages.ROCKSDB_CONNECTING,
		config.RocksdbPath,
	).ConsoleLog()
	opts := grocksdb.NewDefaultOptions()
	opts.SetMaxOpenFiles(-1)
	ro := grocksdb.NewDefaultReadOptions()

	cf, err := grocksdb.ListColumnFamilies(opts, config.RocksdbPath)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(OpenRocksdb),
			err,
			messages.ROCKSDB_FAILED_TO_LIST_COLUMN_FAMILIES,
		).ConsoleLog()
	}
	cfOpts := []*grocksdb.Options{}
	for range cf {
		cfOpts = append(cfOpts, opts)
	}

	db, handles, err := grocksdb.OpenDbAsSecondaryColumnFamilies(
		opts,
		config.RocksdbPath,
		config.RocksdbSecondaryPath,
		cf,
		cfOpts,
	)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(OpenRocksdb),
			err,
			messages.ROCKSDB_FAILED_TO_CONNECT,
			config.RocksdbPath,
		).ConsoleLog()
	}

	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_SUCCESS,
		"",
		nil,
		messages.ROCKSDB_CONNECTED,
	).ConsoleLog()

	return &RockClient{
		db,
		handles,
		opts,
		ro,
	}
}

// GetLookupKeyForBlockHeight returns the rocksdb lookup key for a given block height
func (rc *RockClient) GetLookupKeyForBlockHeight(blockHeight int) []byte {
	blockKey := blockHeightToKey(blockHeight)
	response, err := rc.db.GetCF(rc.ro, rc.columnHandles[COL_KEY_LOOKUP], blockKey)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(rc.GetLookupKeyForBlockHeight),
			err,
			messages.ROCKSDB_FAILED_LOOKUP_KEY,
			blockHeight,
		).ConsoleLog()
	}
	defer response.Free()
	returnedData := []byte{}
	returnedData = append(returnedData, response.Data()...)
	return returnedData
}

func blockHeightToKey(blockHeight int) []byte {
	return []byte{
		byte(blockHeight >> 24),
		byte((blockHeight >> 16) & 0xff),
		byte((blockHeight >> 8) & 0xff),
		byte(blockHeight & 0xff),
	}
}

// GetBodyForBlockLookupKey retrieves the raw block body being given a lookup key
func (rc *RockClient) GetBodyForBlockLookupKey(key []byte) []byte {
	body, err := rc.db.GetCF(rc.ro, rc.columnHandles[COL_BODY], key)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(rc.GetBodyForBlockLookupKey),
			err,
			messages.ROCKSDB_FAILED_BODY,
		).ConsoleLog()
	}
	defer body.Free()
	returnedData := []byte{}
	returnedData = append(returnedData, body.Data()...)
	return returnedData
}

// GetLastBlockSynced gets the last synced block from the rocksdb database
func (rc *RockClient) GetLastBlockSynced() int {
	lastElement, err := rc.db.GetCF(rc.ro, rc.columnHandles[COL_META], []byte("final"))
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(rc.GetLastBlockSynced),
			err,
			messages.ROCKSDB_FAILED_TO_GET_LAST_SYNCED_BLOCK,
		).ConsoleLog()
	}
	defer lastElement.Free()

	hexIndex := hex.EncodeToString(lastElement.Data()[0:4])
	maxBlockHeight, err := strconv.ParseInt(hexIndex, 16, 64)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(rc.GetLastBlockSynced),
			err,
			"",
		).ConsoleLog()
	}
	return int(maxBlockHeight)
}

func (rc *RockClient) GetBlockHash(height int) string {
	lk := rc.GetLookupKeyForBlockHeight(height)
	hash := hex.EncodeToString(lk[4:])
	return hash
}

// GetStateTrieNode returns a trie node from rocksdb being given a key
func (rc *RockClient) GetStateTrieNode(stateLookupKey []byte) []byte {
	trieNode, err := rc.db.GetCF(rc.ro, rc.columnHandles[COL_STATE], stateLookupKey)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(rc.GetStateTrieNode),
			err,
			messages.ROCKSDB_FAILED_TRIE_NODE_DB,
		).ConsoleLog()
	}
	defer trieNode.Free()
	returnedData := []byte{}
	returnedData = append(returnedData, trieNode.Data()...)
	return returnedData
}

// GetHeaderForBlockLookupKey retrieves the block header from rocksdb
func (rc *RockClient) GetHeaderForBlockLookupKey(key []byte) []byte {
	header, err := rc.db.GetCF(rc.ro, rc.columnHandles[COL_HEADER], key)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(rc.GetHeaderForBlockLookupKey),
			err,
			messages.ROCKSDB_FAILED_HEADER,
		).ConsoleLog()
	}
	defer header.Free()

	returnedHeader := []byte{}
	returnedHeader = append(returnedHeader, header.Data()...)

	return returnedHeader
}

// CatchUpWithPrimary is used to synchronize the secondary rocksdb instance with the primary/live instance
func (rc *RockClient) CatchUpWithPrimary() {
	err := rc.db.TryCatchUpWithPrimary()
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(rc.CatchUpWithPrimary),
			err,
			messages.ROCKSDB_FAILED_TO_UPDATE_SECONDARY,
		).ConsoleLog()
	}
}

func (rc *RockClient) GetGenesisHash() string {
	rawGenesis, err := rc.db.GetCF(rc.ro, rc.columnHandles[COL_META], []byte("gen"))
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(rc.GetGenesisHash),
			err,
			messages.ROCKSDB_FAILED_GENESIS,
		).ConsoleLog()
	}
	defer rawGenesis.Free()

	return hex.EncodeToString(rawGenesis.Data())
}

func (rc *RockClient) Close() {
	rc.db.Close()
}
