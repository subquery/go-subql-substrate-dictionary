package specversion

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go-dictionary/internal/messages"
	"net/http"
	"strconv"
	"sync"

	"go-dictionary/internal/db/postgres"
	"go-dictionary/internal/db/rocksdb"

	"github.com/itering/scale.go/types"
	"github.com/itering/scale.go/utiles"
	"github.com/itering/substrate-api-rpc/rpc"

	scalecodec "github.com/itering/scale.go"
)

type (
	SpecVersionClient struct {
		sync.RWMutex
		lastBlock            int //last indexed block by the node we interrogate
		rocksdbClient        *rocksdb.RockClient
		pgClient             specvRepoClient
		httpEndpoint         string
		specVersionRangeList SpecVersionRangeList
	}

	specvRepoClient struct {
		*postgres.PostgresClient
	}
)

const (
	firstChainBlock = 1
)

var (
	SPEC_VERSION_MESSAGE = `{"id":1,"method":"chain_getRuntimeVersion","params":["%s"],"jsonrpc":"2.0"}`
)

func NewSpecVersionClient(
	lastBlock int,
	rocksdbClient *rocksdb.RockClient,
	pgClient *postgres.PostgresClient,
	httpRpcEndpoint string) *SpecVersionClient {
	return &SpecVersionClient{
		lastBlock:     lastBlock,
		rocksdbClient: rocksdbClient,
		pgClient:      specvRepoClient{pgClient},
		httpEndpoint:  httpRpcEndpoint,
	}
}

// Run initializes the spec version client; it should be executed before running any other client
func (specVClient *SpecVersionClient) Run() {
	var (
		lastBlockForCurrentRange int // the last block of the current searched spec version
		actualLastBlock          int
		start                    int // the starting block for the current spec version
		currentSpecVersion       int // the currently searched spec version
		lastSavedDbBlockInfo     *SpecVersionRange
		err                      error
		isDataInDb               bool
	)
	insertPosition := 0
	shouldInsert := false

	lastSavedDbBlockInfo, isDataInDb = specVClient.recoverLastBlock()

	// if there are spec versions in db get all of them
	if isDataInDb {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_INFO,
			"",
			nil,
			SPEC_VERSION_RECOVERED,
		).ConsoleLog()
		specVClient.specVersionRangeList = specVClient.pgClient.getAllSpecVersionData()
		specVClient.specVersionRangeList.FillLast(specVClient.lastBlock)
		insertPosition = len(specVClient.specVersionRangeList)

		for i := 0; i < len(specVClient.specVersionRangeList); i++ {
			specVClient.specVersionRangeList[i].Meta = specVClient.getMetadata(specVClient.specVersionRangeList[i].First)
		}
	}

	// if last block in db is equal to last block indexed by node, simply return the info about the blocks in db
	if lastSavedDbBlockInfo.First == specVClient.lastBlock {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_INFO,
			"",
			nil,
			SPEC_VERSION_UP_TO_DATE,
		).ConsoleLog()
		return
	}

	start = lastSavedDbBlockInfo.First // start getting spec version info from the first block of the last range saved in db
	lastBlockForCurrentRange = start
	currentSpecVersion, err = strconv.Atoi(lastSavedDbBlockInfo.SpecVersion)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(specVClient.Run),
			err,
			messages.FAILED_ATOI,
		).ConsoleLog()
	}

	for lastBlockForCurrentRange != specVClient.lastBlock {
		lastBlockForCurrentRange = specVClient.getLastBlockForSpecVersion(currentSpecVersion, start, specVClient.lastBlock)

		if lastBlockForCurrentRange != specVClient.lastBlock {
			actualLastBlock = lastBlockForCurrentRange + 1
		} else {
			actualLastBlock = lastBlockForCurrentRange
		}

		currentSpecVersionString := fmt.Sprintf("%d", currentSpecVersion)
		if len(specVClient.specVersionRangeList) == 0 || specVClient.specVersionRangeList[len(specVClient.specVersionRangeList)-1].SpecVersion != currentSpecVersionString {
			first := firstChainBlock
			if len(specVClient.specVersionRangeList) != 0 {
				first = specVClient.specVersionRangeList[len(specVClient.specVersionRangeList)-1].Last + 1
			}
			specVClient.specVersionRangeList = append(specVClient.specVersionRangeList, SpecVersionRange{
				Last:        actualLastBlock,
				First:       first,
				SpecVersion: currentSpecVersionString,
				Meta:        specVClient.getMetadata(first),
			})
			shouldInsert = true
		} else {
			specVClient.specVersionRangeList[len(specVClient.specVersionRangeList)-1].Last = actualLastBlock
		}

		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_INFO,
			"",
			nil,
			SPEC_VERSION_RETRIEVED,
			currentSpecVersion,
			actualLastBlock,
		).ConsoleLog()

		if lastBlockForCurrentRange != specVClient.lastBlock {
			start = lastBlockForCurrentRange + 1
			currentSpecVersion = specVClient.getSpecVersion(start)
		}
	}

	if shouldInsert {
		specVClient.pgClient.insertSpecVersionsList(specVClient.specVersionRangeList[insertPosition:])
	}
}

// GetSpecVersion downloads the spec version for a block using the HTTP RPC endpoint
func (specVClient *SpecVersionClient) getSpecVersion(height int) int {
	hash := specVClient.rocksdbClient.GetBlockHash(height)
	msg := fmt.Sprintf(SPEC_VERSION_MESSAGE, hexPrefix+hash)
	reqBody := bytes.NewBuffer([]byte(msg))
	resp, postErr := http.Post(specVClient.httpEndpoint, "application/json", reqBody)
	if postErr != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(specVClient.getSpecVersion),
			postErr,
			SPEC_VERSION_FAILED_POST_MESSAGE,
			height,
		).ConsoleLog()
	}

	v := &rpc.JsonRpcResult{}
	jsonDecodeErr := json.NewDecoder(resp.Body).Decode(&v)
	if jsonDecodeErr != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(specVClient.getSpecVersion),
			jsonDecodeErr,
			SPEC_VERSION_FAILED_TO_DECODE,
			height,
		).ConsoleLog()
	}

	return v.ToRuntimeVersion().SpecVersion
}

// GetSpecName return spec version name
func (specVClient *SpecVersionClient) GetSpecName() string {
	hash := specVClient.rocksdbClient.GetBlockHash(firstChainBlock)
	msg := fmt.Sprintf(SPEC_VERSION_MESSAGE, hash)
	reqBody := bytes.NewBuffer([]byte(msg))
	resp, postErr := http.Post(specVClient.httpEndpoint, "application/json", reqBody)
	if postErr != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(specVClient.GetSpecName),
			postErr,
			SPEC_VERSION_FAILED_POST_MESSAGE,
			firstChainBlock,
		).ConsoleLog()
	}

	v := &rpc.JsonRpcResult{}
	jsonDecodeErr := json.NewDecoder(resp.Body).Decode(&v)
	if jsonDecodeErr != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(specVClient.GetSpecName),
			jsonDecodeErr,
			SPEC_VERSION_FAILED_TO_DECODE,
			firstChainBlock,
		).ConsoleLog()
	}

	return v.ToRuntimeVersion().SpecName
}

// GetLastBlockForSpecVersion uses binary search to look between start and end block heights for the last node for a spec version
func (specVClient *SpecVersionClient) getLastBlockForSpecVersion(specVersion, start, end int) int {
	s := start
	e := end

	for {
		mid := (s + (e - 1)) / 2

		if e == s {
			return e
		}

		if e-1 == s {
			spec := specVClient.getSpecVersion(e)

			if spec == specVersion {
				return e
			}

			return s
		}

		mSpec := specVClient.getSpecVersion(mid)

		if mSpec > specVersion {
			e = mid - 1
			continue
		}

		if mSpec < specVersion {
			s = mid + 1
		}

		if mSpec == specVersion {
			afterMSpec := specVClient.getSpecVersion(mid + 1)

			if afterMSpec > specVersion {
				return mid
			}

			s = mid + 1
			continue
		}
	}
}

// recoverLastBlock tries to recover the last block spec version saved in db
func (specVClient *SpecVersionClient) recoverLastBlock() (*SpecVersionRange, bool) {
	var (
		lastBlockSavedInDb *SpecVersionRange
	)

	lastBlockSavedInDb = specVClient.pgClient.getLastSolvedBlockAndSpecVersion()
	if lastBlockSavedInDb.First == -1 && lastBlockSavedInDb.SpecVersion == "-1" {
		// if no block spec version info was found in db, start form the beginning of the chain
		lastBlockSavedInDb.First = firstChainBlock
		lastBlockSavedInDb.SpecVersion = fmt.Sprintf("%d", specVClient.getSpecVersion(firstChainBlock))
		return lastBlockSavedInDb, false
	}

	return lastBlockSavedInDb, true
}

// getAllDbSpecVersions retrieves all blocks specversions from db
func (specVClient *SpecVersionClient) getAllDbSpecVersions() SpecVersionRangeList {
	specVersions := specVClient.pgClient.getAllSpecVersionData()
	return specVersions
}

// getMetadata retrieves the metadata structure for a block height
func (specVClient *SpecVersionClient) getMetadata(blockHeight int) *types.MetadataStruct {
	hash := specVClient.rocksdbClient.GetBlockHash(blockHeight)
	reqBody := bytes.NewBuffer([]byte(rpc.StateGetMetadata(1, hexPrefix+hash)))
	resp, err := http.Post(specVClient.httpEndpoint, "application/json", reqBody)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(specVClient.getMetadata),
			err,
			META_FAILED_POST_MESSAGE,
			blockHeight,
		).ConsoleLog()
	}
	defer resp.Body.Close()

	metaRawBody := &rpc.JsonRpcResult{}
	json.NewDecoder(resp.Body).Decode(metaRawBody)
	metaBodyString, err := metaRawBody.ToString()
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(specVClient.getMetadata),
			err,
			META_FAILED_TO_DECODE_BODY,
			blockHeight,
		).ConsoleLog()
	}

	metaDecoder := scalecodec.MetadataDecoder{}
	metaDecoder.Init(utiles.HexToBytes(metaBodyString))
	err = metaDecoder.Process()
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(specVClient.getMetadata),
			err,
			META_FAILED_SCALE_DECODE,
			blockHeight,
		).ConsoleLog()
	}

	return &metaDecoder.Metadata
}

// GetSpecVersionAndMetadata returns the spec version and the metadata for a given block height
func (specVClient *SpecVersionClient) GetSpecVersionAndMetadata(blockHeight int) *SpecVersionRange {
	specVClient.RLock()
	defer specVClient.RUnlock()
	return specVClient.specVersionRangeList.getSpecVersionForBlock(blockHeight)
}

// UpdateLive updates the spec version and metadata in live mode
func (specVClient *SpecVersionClient) UpdateLive(lastBlock int) {
	if lastBlock <= specVClient.lastBlock {
		return
	}

	var (
		actualLastBlock int
		shouldInsert    bool
	)
	insertPosition := len(specVClient.specVersionRangeList)

	lastSavedBlockInfo := specVClient.specVersionRangeList[len(specVClient.specVersionRangeList)-1]
	lastBlockForCurrentRange := lastSavedBlockInfo.Last
	start := lastBlockForCurrentRange
	currentSpecVersion, err := strconv.Atoi(lastSavedBlockInfo.SpecVersion)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(specVClient.Run),
			err,
			messages.FAILED_ATOI,
		).ConsoleLog()
	}

	for lastBlockForCurrentRange != lastBlock {
		lastBlockForCurrentRange = specVClient.getLastBlockForSpecVersion(currentSpecVersion, start, lastBlock)

		if lastBlockForCurrentRange != lastBlock {
			actualLastBlock = lastBlockForCurrentRange + 1
		} else {
			actualLastBlock = lastBlockForCurrentRange
		}

		currentSpecVersionString := fmt.Sprintf("%d", currentSpecVersion)
		specVClient.Lock()
		if len(specVClient.specVersionRangeList) == 0 || specVClient.specVersionRangeList[len(specVClient.specVersionRangeList)-1].SpecVersion != currentSpecVersionString {
			first := firstChainBlock
			if len(specVClient.specVersionRangeList) != 0 {
				first = specVClient.specVersionRangeList[len(specVClient.specVersionRangeList)-1].Last + 1
			}
			specVClient.specVersionRangeList = append(specVClient.specVersionRangeList, SpecVersionRange{
				Last:        actualLastBlock,
				First:       first,
				SpecVersion: currentSpecVersionString,
				Meta:        specVClient.getMetadata(first),
			})
			shouldInsert = true
		} else {
			specVClient.specVersionRangeList[len(specVClient.specVersionRangeList)-1].Last = actualLastBlock
		}
		specVClient.Unlock()

		if lastBlockForCurrentRange != lastBlock {
			start = lastBlockForCurrentRange + 1
			currentSpecVersion = specVClient.getSpecVersion(start)
		}
	}

	if shouldInsert {
		specVClient.RLock()
		specVClient.pgClient.insertSpecVersionsList(specVClient.specVersionRangeList[insertPosition:])
		specVClient.RUnlock()
	}
}
