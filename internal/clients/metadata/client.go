package metadata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go-dictionary/internal/clients/specversion"
	"go-dictionary/internal/db/rocksdb"
	"go-dictionary/internal/messages"
	"net/http"

	scalecodec "github.com/itering/scale.go"
	"github.com/itering/scale.go/utiles"
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

// GetMetadata retrieves the metadata for a list of spec versions
func (metaClient *MetadataClient) GetMetadata(specvRangeList specversion.SpecVersionRangeList) *messages.DictionaryMessage {
	for i := range specvRangeList {
		hash, msg := metaClient.rocksdbClient.GetBlockHash(specvRangeList[i].Last)
		if msg != nil {
			return msg
		}

		reqBody := bytes.NewBuffer([]byte(rpc.StateGetMetadata(1, hash)))
		resp, err := http.Post(metaClient.httpEndpoint, "application/json", reqBody)
		if err != nil {
			return nil
		}
		defer resp.Body.Close()

		metaBody := &rpc.JsonRpcResult{}
		json.NewDecoder(resp.Body).Decode(metaBody)

		mbd, _ := metaBody.ToString()

		mDecoder := scalecodec.MetadataDecoder{}
		mDecoder.Init(utiles.HexToBytes(mbd))
		mDecoder.Process()

		fmt.Println(specvRangeList[i].SpecVersion, mDecoder.Version, mDecoder.VersionNumber)
	}

	return nil
}
