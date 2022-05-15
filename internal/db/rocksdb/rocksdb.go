package rocksdb

import (
	"encoding/hex"
	"go-dictionary/internal/messages"
	"log"
	"strconv"
	"sync"

	"github.com/linxGnu/grocksdb"
)

const polkaAddressPrefix = "00"
const SS58PRE = "53533538505245"

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
func OpenRocksdb(path, secondaryPath string) (*RockClient, *messages.DictionaryMessage) {
	messages.NewDictionaryMessage(messages.LOG_LEVEL_INFO, "", nil, messages.ROCKSDB_CONNECTING, path).ConsoleLog()
	opts := grocksdb.NewDefaultOptions()
	opts.SetMaxOpenFiles(-1)
	ro := grocksdb.NewDefaultReadOptions()

	cf, err := grocksdb.ListColumnFamilies(opts, path)
	if err != nil {
		return nil, messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(OpenRocksdb),
			err,
			messages.ROCKSDB_FAILED_TO_LIST_COLUMN_FAMILIES,
		)
	}
	cfOpts := []*grocksdb.Options{}
	for range cf {
		cfOpts = append(cfOpts, opts)
	}

	db, handles, err := grocksdb.OpenDbAsSecondaryColumnFamilies(
		opts,
		path,
		secondaryPath,
		cf,
		cfOpts,
	)
	if err != nil {
		return nil, messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(OpenRocksdb),
			err,
			messages.ROCKSDB_FAILED_TO_CONNECT,
			path,
		)
	}

	messages.NewDictionaryMessage(messages.LOG_LEVEL_SUCCESS, "", nil, messages.ROCKSDB_CONNECTED).ConsoleLog()
	return &RockClient{
		db,
		handles,
		opts,
		ro,
	}, nil
}

func (rc *RockClient) GetLookupKeyForBlockHeight(blockHeight int) ([]byte, *messages.DictionaryMessage) {
	blockKey := BlockHeightToKey(blockHeight)
	response, err := rc.db.GetCF(rc.ro, rc.columnHandles[COL_KEY_LOOKUP], blockKey)
	if err != nil {
		return []byte{}, messages.NewDictionaryMessage(messages.LOG_LEVEL_ERROR, messages.GetComponent(rc.GetLookupKeyForBlockHeight), err, messages.ROCKSDB_FAILED_LOOKUP_KEY, blockHeight)
	}
	return response.Data(), nil
}

func BlockHeightToKey(blockHeight int) []byte {
	return []byte{
		byte(blockHeight >> 24),
		byte((blockHeight >> 16) & 0xff),
		byte((blockHeight >> 8) & 0xff),
		byte(blockHeight & 0xff),
	}
}

func (rc *RockClient) GetHeaderForBlockLookupKey(key []byte) ([]byte, error) {
	header, err := rc.db.GetCF(rc.ro, rc.columnHandles[COL_HEADER], key)
	return header.Data(), err
}

func (rc *RockClient) GetBodyForBlockLookupKey(key []byte) ([]byte, error) {
	body, err := rc.db.GetCF(rc.ro, rc.columnHandles[COL_BODY], key)
	return body.Data(), err
}

// GetLastBlockSynced gets the last synced block from the rocksdb database
func (rc *RockClient) GetLastBlockSynced() (int, *messages.DictionaryMessage) {
	lastElement, err := rc.db.GetCF(rc.ro, rc.columnHandles[COL_META], []byte("final"))
	if err != nil {
		return 0, messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(rc.GetLastBlockSynced),
			err,
			messages.ROCKSDB_FAILED_TO_GET_LAST_SYNCED_BLOCK,
		)
	}

	hexIndex := hex.EncodeToString(lastElement.Data()[0:4])
	maxBlockHeight, err := strconv.ParseInt(hexIndex, 16, 64)
	if err != nil {
		return 0, messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(rc.GetLastBlockSynced),
			err,
			"",
		)
	}
	return int(maxBlockHeight), nil
}

func (rc *RockClient) GetBlockHash(height int) (string, *messages.DictionaryMessage) {
	lk, err := rc.GetLookupKeyForBlockHeight(height)
	if err != nil {
		return "", err
	}
	hash := hex.EncodeToString(lk[4:])
	return hash, nil
}

func (rc *RockClient) StartProcessing(bq *JobQueueBody, hq *JobQueueHeader, lastBlockSynced int) {
	log.Println("[INFO] LAST BLOCK SYNCED -", lastBlockSynced)

	preProcessChannel := make(chan int, 10000000)

	var pWg sync.WaitGroup
	pWg.Add(1)
	go rc.PreProcessWorker(&pWg, bq, hq, preProcessChannel)

	for i := 0; i < lastBlockSynced; i++ {
		preProcessChannel <- i
	}
	close(preProcessChannel)

	pWg.Wait()
}

func (rc *RockClient) GetHeaderRaw(wg *sync.WaitGroup, headerJob *HeaderJob, hq *JobQueueHeader) {
	defer wg.Done()
	header, err := rc.GetHeaderForBlockLookupKey(headerJob.BlockLookupKey)
	if err != nil {
		log.Println("[ERR]", err, "- could not get the header for block lookup key!")
	}
	headerJob.BlockHeader = header
	hq.Submit(headerJob)
}

func (rc *RockClient) GetBodyRaw(wg *sync.WaitGroup, bodyJob *BodyJob, bq *JobQueueBody) {
	defer wg.Done()
	body, err := rc.GetBodyForBlockLookupKey(bodyJob.BlockLookupKey)
	if err != nil {
		log.Println("[ERR]", err, "- could not get the body for block lookup key!!")
	}
	bodyJob.BlockBody = body
	bq.Submit(bodyJob)
}

func (rc *RockClient) PreProcessWorker(wg *sync.WaitGroup, bq *JobQueueBody, hq *JobQueueHeader, ppc chan int) {
	log.Println("[+] Starting PreProcessWorker!")
	defer wg.Done()

	for blockHeight := range ppc {
		bodyJob := BodyJob{}
		headerJob := HeaderJob{}

		bodyJob.BlockHeight = blockHeight
		headerJob.BlockHeight = blockHeight

		key, err := rc.GetLookupKeyForBlockHeight(blockHeight)
		if err != nil {
			log.Println("[ERR]", err, blockHeight, "- could not get the lookup key!")
		}

		bodyJob.BlockLookupKey = key
		headerJob.BlockLookupKey = key

		hash := hex.EncodeToString(key[4:])
		bodyJob.BlockHash = hash
		headerJob.BlockHash = hash

		var rawWg sync.WaitGroup
		rawWg.Add(2)
		// rawWg.Add(1)
		go rc.GetHeaderRaw(&rawWg, &headerJob, hq)
		go rc.GetBodyRaw(&rawWg, &bodyJob, bq)
		rawWg.Wait()
	}
	close(hq.internalQueue)
	close(bq.internalQueue)
	log.Println("[-] Exiting PreProcessWorker...")
}

func (rc *RockClient) Close() {
	rc.db.Close()
}
