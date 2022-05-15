package utils

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/itering/scale.go/types"
	"github.com/itering/substrate-api-rpc/metadata"
)

type SpecVersionRange struct {
	SpecVersion int                   `json:"spec_version"`
	First       int                   `json:"first"` //first block for a spec version
	Last        int                   `json:"last"`  //last block for a spec version
	Meta        *types.MetadataStruct `json:"-"`
	Instant     *metadata.Instant     `json:"-"`
}

type SpecVersionRangeList []SpecVersionRange

func GetSpecVersionsFromFile() (SpecVersionRangeList, error) {
	specFile := os.Getenv("SPEC_VERSION_RANGE_FILE")

	rawSpecs, err := ioutil.ReadFile(specFile)
	if err != nil {
		fmt.Println("Error reading spec ranges file for spec version", err)
		return nil, err
	}

	var specRanges SpecVersionRangeList
	err = json.Unmarshal(rawSpecs, &specRanges)
	if err != nil {
		fmt.Println("Error reading spec ranges file for spec version", err)
		return nil, err
	}

	for idx, spec := range specRanges {
		meta, instant, err := getMetaForSpecVersion(spec.SpecVersion)
		if err != nil {
			return nil, err
		}
		specRanges[idx].Meta = meta
		specRanges[idx].Instant = instant
	}

	return specRanges, nil
}

func (s SpecVersionRangeList) GetBlockSpecVersion(blockHeight int) int {
	if blockHeight == 0 {
		return 0
	}
	for idx, spec := range s {
		if blockHeight == spec.First {
			return s[idx-1].SpecVersion
		}
		//check only last version as the spec versions are in ascending order
		if blockHeight <= spec.Last {
			return spec.SpecVersion
		}
	}
	return -1
}

func (s SpecVersionRangeList) GetBlockSpecVersionAndInstant(blockHeight int) (int, *metadata.Instant) {
	if blockHeight == 0 {
		return 0, s[0].Instant
	}
	for idx, spec := range s {
		if blockHeight == spec.First {
			return s[idx-1].SpecVersion, s[idx-1].Instant
		}
		//check only last version as the spec versions are in ascending order
		if blockHeight <= spec.Last {
			return spec.SpecVersion, spec.Instant
		}
	}
	return -1, nil
}
