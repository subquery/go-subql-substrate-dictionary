package utils

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"

	scalecodec "github.com/itering/scale.go"
	"github.com/itering/scale.go/types"
	"github.com/itering/scale.go/utiles"
	"github.com/itering/substrate-api-rpc/metadata"
)

//load metadata from the downlaoded files
func getMetaForSpecVersion(specVersion int) (*types.MetadataStruct, *metadata.Instant, error) {
	metaPath := os.Getenv("METADATA_FILES_PARENT")

	fullPath := path.Join(metaPath, strconv.Itoa(specVersion))
	rawMeta, err := ioutil.ReadFile(fullPath)
	if err != nil {
		fmt.Println("Error reading metadata file for spec version", specVersion, err)
		return nil, nil, err
	}
	rawString := string(rawMeta)

	m := scalecodec.MetadataDecoder{}
	m.Init(utiles.HexToBytes(rawString))
	err = m.Process()
	if err != nil {
		fmt.Println("Error processing metadata for spec version", specVersion, err)
		return nil, nil, err
	}

	rawInstant := metadata.RuntimeRaw{Spec: specVersion, Raw: string(rawString)}
	metaInstant := metadata.Process(&rawInstant)

	return &m.Metadata, metaInstant, err
}
