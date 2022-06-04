package metadata

import (
	"bytes"
	"encoding/json"
	"go-dictionary/internal/clients/specversion"
	"go-dictionary/internal/db/rocksdb"
	"go-dictionary/internal/messages"
	"net/http"
	"strconv"

	scalecodec "github.com/itering/scale.go"
	"github.com/itering/scale.go/utiles"
	"github.com/itering/substrate-api-rpc/metadata"
	"github.com/itering/substrate-api-rpc/rpc"
)

type (
	MetadataClient struct {
		rocksdbClient *rocksdb.RockClient
		httpEndpoint  string
	}
)

func NewMetadataClient(
	rocksdbClient *rocksdb.RockClient,
	httpEndpoint string,
) *MetadataClient {
	return &MetadataClient{
		rocksdbClient: rocksdbClient,
		httpEndpoint:  httpEndpoint,
	}
}

// GetMetadata retrieves the metadata for a list of spec versions and maps that metadata to the spec version number
func (metaClient *MetadataClient) GetMetadata(specvRangeList specversion.SpecVersionRangeList) (map[string]*DictionaryMetadata, *messages.DictionaryMessage) {
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_INFO,
		"",
		nil,
		messages.META_STARTING,
	).ConsoleLog()

	specVersionMetadataMap := make(map[string]*DictionaryMetadata, len(specvRangeList))

	for _, specV := range specvRangeList {
		specVersionNumber, _ := strconv.Atoi(specV.SpecVersion) // ignore the error as we should have a good integer value at this point
		meta, msg := metaClient.getMetadata(specV.First, specVersionNumber)
		if msg != nil {
			return nil, msg
		}
		specVersionMetadataMap[specV.SpecVersion] = meta
	}

	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_SUCCESS,
		"",
		nil,
		messages.META_FINISHED,
	)
	return specVersionMetadataMap, nil
}

// getMetadata retrieves the metadata instant for a block height
func (metaClient *MetadataClient) getMetadata(blockHeight, specVersion int) (*DictionaryMetadata, *messages.DictionaryMessage) {
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

	rawMetaRuntime := metadata.RuntimeRaw{
		Spec: specVersion,
		Raw:  metaBodyString,
	}
	metaInstant := metadata.Process(&rawMetaRuntime)

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

	return &DictionaryMetadata{
		Meta:        &metaDecoder.Metadata,
		MetaInstant: metaInstant,
	}, nil
}
