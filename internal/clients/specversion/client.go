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

	"github.com/itering/substrate-api-rpc/rpc"
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

func (specVClient *SpecVersionClient) Run() (SpecVersionRangeList, *messages.DictionaryMessage) {
	var (
		msg                      *messages.DictionaryMessage
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

	lastSavedDbBlockInfo, isDataInDb, msg = specVClient.recoverLastBlock(fmt.Sprintf("%d", specVClient.startingBlockSpecVersion))
	if msg != nil {
		return specList, msg
	}

	// if there are spec versions in db get all of them
	if isDataInDb {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_INFO,
			"",
			nil,
			messages.SPEC_VERSION_RECOVERED,
		).ConsoleLog()
		specList, msg = specVClient.getAllDbSpecVersions()
		specList.FillLast(specVClient.lastBlock)
		insertPosition = len(specList)
		if msg != nil {
			return specList, msg
		}
	}

	// if last block in db is equal to last block indexed by node, simply return the info about the blocks in db
	if lastSavedDbBlockInfo.First == specVClient.lastBlock {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_INFO,
			"",
			nil,
			messages.SPEC_VERSION_UP_TO_DATE,
		).ConsoleLog()
		return specList, nil
	}

	start = lastSavedDbBlockInfo.First // start getting spec version info from the first block of the last range saved in db
	lastBlockForCurrentRange = start
	currentSpecVersion, err = strconv.Atoi(lastSavedDbBlockInfo.SpecVersion)
	if err != nil {
		return specList, messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(specVClient.Run),
			err,
			messages.FAILED_ATOI,
		)
	}

	for lastBlockForCurrentRange != specVClient.lastBlock {
		lastBlockForCurrentRange, msg = specVClient.getLastBlockForSpecVersion(currentSpecVersion, start, specVClient.lastBlock)
		if msg != nil {
			return specList, msg
		}

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
			currentSpecVersion, msg = specVClient.getSpecVersion(start)
			if msg != nil {
				return specList, msg
			}
		}
	}

	if shouldInsert {
		msg = specVClient.pgClient.insertSpecVersionsList(specList[insertPosition:])
		if msg != nil {
			return specList, msg
		}
	}

	return specList, nil
}

// GetSpecVersion downloads the spec version for a block using the HTTP RPC endpoint
func (specVClient *SpecVersionClient) getSpecVersion(height int) (int, *messages.DictionaryMessage) {
	hash, err := specVClient.rocksdbClient.GetBlockHash(height)
	if err != nil {
		return -1, err
	}

	msg := fmt.Sprintf(SPEC_VERSION_MESSAGE, hash)
	reqBody := bytes.NewBuffer([]byte(msg))

	resp, postErr := http.Post(specVClient.httpEndpoint, "application/json", reqBody)
	if postErr != nil {
		return -1, messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(specVClient.getSpecVersion),
			postErr,
			messages.SPEC_VERSION_FAILED_POST_MESSAGE,
			height,
		)
	}

	v := &rpc.JsonRpcResult{}
	jsonDecodeErr := json.NewDecoder(resp.Body).Decode(&v)
	if jsonDecodeErr != nil {
		return -1, messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(specVClient.getSpecVersion),
			jsonDecodeErr,
			messages.SPEC_VERSION_FAILED_TO_DECODE,
			height,
		)
	}

	return v.ToRuntimeVersion().SpecVersion, nil
}

// GetLastBlockForSpecVersion uses binary search to look between start and end block heights for the last node for a spec version
func (specVClient *SpecVersionClient) getLastBlockForSpecVersion(specVersion, start, end int) (int, *messages.DictionaryMessage) {
	s := start
	e := end

	for {
		mid := (s + (e - 1)) / 2

		if e == s {
			return e, nil
		}

		if e-1 == s {
			spec, err := specVClient.getSpecVersion(e)
			if err != nil {
				return -1, err
			}

			if spec == specVersion {
				return e, nil
			}

			return s, nil
		}

		mSpec, err := specVClient.getSpecVersion(mid)
		if err != nil {
			return -1, err
		}

		if mSpec > specVersion {
			e = mid - 1
			continue
		}

		if mSpec < specVersion {
			s = mid + 1
		}

		if mSpec == specVersion {
			afterMSpec, err := specVClient.getSpecVersion(mid + 1)
			if err != nil {
				return -1, err
			}

			if afterMSpec > specVersion {
				return mid, nil
			}

			s = mid + 1
			continue
		}
	}
}

// recoverLastBlock tries to recover the last block spec version saved in db
func (specVClient *SpecVersionClient) recoverLastBlock(firstSpecVersion string) (*SpecVersionRange, bool, *messages.DictionaryMessage) {
	var (
		lastBlockSavedInDb *SpecVersionRange
		msg                *messages.DictionaryMessage
	)

	lastBlockSavedInDb, msg = specVClient.pgClient.getLastSolvedBlockAndSpecVersion()
	if msg != nil {
		if msg.LogLevel == messages.LOG_LEVEL_ERROR {
			return nil, false, msg
		}

		// if no block spec version info was found in db, start form the beginning of the chain
		if msg.LogLevel == messages.LOG_LEVEL_INFO {
			msg.ConsoleLog()
			return &SpecVersionRange{SpecVersion: firstSpecVersion, First: firstChainBlock}, false, nil
		}
	}

	return lastBlockSavedInDb, true, nil
}

// getAllDbSpecVersions retrieves all blocks specversions from db
func (specVClient *SpecVersionClient) getAllDbSpecVersions() (SpecVersionRangeList, *messages.DictionaryMessage) {
	specVersions, msg := specVClient.pgClient.getAllSpecVersionData()
	if msg != nil {
		if msg.LogLevel == messages.LOG_LEVEL_ERROR {
			return nil, msg
		}

		if msg.LogLevel == messages.LOG_LEVEL_INFO {
			msg.ConsoleLog()
		}
	}

	return specVersions, nil
}
