package specversion

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go-dictionary/internal/messages"
	"net/http"
	"strconv"

	"go-dictionary/internal/db/postgres"
	"go-dictionary/internal/db/rocksdb"

	"github.com/itering/scale.go/types"
	"github.com/itering/scale.go/utiles"
	"github.com/itering/substrate-api-rpc/rpc"

	scalecodec "github.com/itering/scale.go"
)

type (
	SpecVersionClient struct {
		startingBlockSpecVersion int //spec version for the first block (block #0)
		lastBlock                int //last indexed block by the node we interrogate
		rocksdbClient            *rocksdb.RockClient
		pgClient                 specvRepoClient
		httpEndpoint             string
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
	startBlockSpecVersion int,
	lastBlock int,
	rocksdbClient *rocksdb.RockClient,
	pgClient *postgres.PostgresClient,
	httpRpcEndpoint string) *SpecVersionClient {
	return &SpecVersionClient{
		startingBlockSpecVersion: startBlockSpecVersion,
		lastBlock:                lastBlock,
		rocksdbClient:            rocksdbClient,
		pgClient:                 specvRepoClient{pgClient},
		httpEndpoint:             httpRpcEndpoint,
	}
}

func (specVClient *SpecVersionClient) Run() SpecVersionRangeList {
	var (
		lastBlockForCurrentRange int // the last block of the current searched spec version
		actualLastBlock          int
		start                    int // the starting block for the current spec version
		currentSpecVersion       int // the currently searched spec version
		lastSavedDbBlockInfo     *SpecVersionRange
		err                      error
		isDataInDb               bool
	)
	specList := SpecVersionRangeList{}
	insertPosition := 0
	shouldInsert := false

	lastSavedDbBlockInfo, isDataInDb = specVClient.recoverLastBlock()

	// if there are spec versions in db get all of them
	if isDataInDb {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_INFO,
			"",
			nil,
			messages.SPEC_VERSION_RECOVERED,
		).ConsoleLog()
		specList = specVClient.pgClient.getAllSpecVersionData()
		specList.FillLast(specVClient.lastBlock)
		insertPosition = len(specList)
	}

	// if last block in db is equal to last block indexed by node, simply return the info about the blocks in db
	if lastSavedDbBlockInfo.First == specVClient.lastBlock {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_INFO,
			"",
			nil,
			messages.SPEC_VERSION_UP_TO_DATE,
		).ConsoleLog()
		return specList
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
		if len(specList) == 0 || specList[len(specList)-1].SpecVersion != currentSpecVersionString {
			first := firstChainBlock
			if len(specList) != 0 {
				first = specList[len(specList)-1].Last + 1
			}
			specList = append(specList, SpecVersionRange{
				Last:        actualLastBlock,
				First:       first,
				SpecVersion: currentSpecVersionString,
			})
			shouldInsert = true
		} else {
			specList[len(specList)-1].Last = actualLastBlock
		}

		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_INFO,
			"",
			nil,
			messages.SPEC_VERSION_RETRIEVED,
			currentSpecVersion,
			actualLastBlock,
		).ConsoleLog()

		if lastBlockForCurrentRange != specVClient.lastBlock {
			start = lastBlockForCurrentRange + 1
			currentSpecVersion = specVClient.getSpecVersion(start)
		}
	}

	if shouldInsert {
		specVClient.pgClient.insertSpecVersionsList(specList[insertPosition:])
	}

	return specList
}

// GetSpecVersion downloads the spec version for a block using the HTTP RPC endpoint
func (specVClient *SpecVersionClient) getSpecVersion(height int) int {
	hash := specVClient.rocksdbClient.GetBlockHash(height)

	msg := fmt.Sprintf(SPEC_VERSION_MESSAGE, hash)
	reqBody := bytes.NewBuffer([]byte(msg))

	resp, postErr := http.Post(specVClient.httpEndpoint, "application/json", reqBody)
	if postErr != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(specVClient.getSpecVersion),
			postErr,
			messages.SPEC_VERSION_FAILED_POST_MESSAGE,
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
			messages.SPEC_VERSION_FAILED_TO_DECODE,
			height,
		).ConsoleLog()
	}

	return v.ToRuntimeVersion().SpecVersion
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

// getMetadata retrieves the metadata instant for a block height
func (metaClient *SpecVersionClient) getMetadata(blockHeight, specVersion int) (*types.MetadataStruct, *messages.DictionaryMessage) {
	hash := metaClient.rocksdbClient.GetBlockHash(blockHeight)

	reqBody := bytes.NewBuffer([]byte(rpc.StateGetMetadata(1, hash)))
	resp, err := http.Post(metaClient.httpEndpoint, "application/json", reqBody)
	if err != nil {
		return nil, messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(metaClient.getMetadata),
			err,
			messages.META_FAILED_POST_MESSAGE,
			blockHeight,
		)
	}
	defer resp.Body.Close()

	metaRawBody := &rpc.JsonRpcResult{}
	json.NewDecoder(resp.Body).Decode(metaRawBody)
	metaBodyString, err := metaRawBody.ToString()
	if err != nil {
		return nil, messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(metaClient.getMetadata),
			err,
			messages.META_FAILED_TO_DECODE_BODY,
			blockHeight,
		)
	}

	metaDecoder := scalecodec.MetadataDecoder{}
	metaDecoder.Init(utiles.HexToBytes(metaBodyString))
	err = metaDecoder.Process()
	if err != nil {
		return nil,
			messages.NewDictionaryMessage(
				messages.LOG_LEVEL_ERROR,
				messages.GetComponent(metaClient.getMetadata),
				err,
				messages.META_FAILED_SCALE_DECODE,
				blockHeight,
			)
	}

	return &metaDecoder.Metadata, nil
}
