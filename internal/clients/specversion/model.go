package specversion

import (
	"go-dictionary/internal/messages"

	"github.com/itering/scale.go/types"
)

type (
	SpecVersionRange struct {
		SpecVersion string `json:"spec_version"`
		First       int    `json:"first"` //first block for a spec version
		Last        int    `json:"last"`  //last block for a spec version
		Meta        *types.MetadataStruct
	}

	SpecVersionRangeList []SpecVersionRange
)

// FillLast fills the last block height for each spec version range
func (specVersionRangeList SpecVersionRangeList) FillLast(lastIndexedBlock int) {
	listLen := len(specVersionRangeList)
	for idx := 0; idx < listLen-1; idx++ {
		specVersionRangeList[idx].Last = specVersionRangeList[idx+1].First - 1
	}
	specVersionRangeList[listLen-1].Last = lastIndexedBlock
}

// GetSpecVersionForBlock receives a block height and returns it's spec version
func (specVersionRangeList SpecVersionRangeList) GetSpecVersionForBlock(blockHeight int) *SpecVersionRange {
	for idx, spec := range specVersionRangeList {
		if blockHeight >= spec.First && blockHeight <= spec.Last {
			return &specVersionRangeList[idx]
		}
	}
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_ERROR,
		messages.GetComponent(specVersionRangeList.GetSpecVersionForBlock),
		nil,
		messages.SPEC_VERSION_WRONG_BLOCK,
		blockHeight,
	).ConsoleLog()
	return nil
}
