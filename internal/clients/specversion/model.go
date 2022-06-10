package specversion

import (
	"go-dictionary/internal/messages"

	"github.com/itering/scale.go/types"
)

const (
	// spec version messages
	SPEC_VERSION_FAILED_POST_MESSAGE  = "Failed to get spec version from HTTP endpoint for block %d"
	SPEC_VERSION_FAILED_TO_DECODE     = "Failed to decode spec version for block %d"
	SPEC_VERSION_FAILED_DB_LAST_BLOCK = "Failed to get last spec version block from db"
	SPEC_VERSION_FAILED_DB            = "Failed to get spec version info from db"
	SPEC_VERSION_RETRIEVED            = "Last block height for spec version %d is %d"
	SPEC_VERSION_DB_INSERT            = "Inserting spec verion data in db starting from block %d"
	SPEC_VERSION_DB_SUCCESS           = "Successfully inserted spec version info in db"
	SPEC_VERSION_UP_TO_DATE           = "Spec versions saved in db are up to date"
	SPEC_VERSION_NO_PREVIOUS_WORK     = "No spec version info was saved from previous executions"
	SPEC_VERSION_RECOVERED            = "Spec version recovered from last run"
	SPEC_VERSION_WRONG_BLOCK          = "Cannot get spec version for block %d"

	// metadata messages
	META_FAILED_POST_MESSAGE   = "Failed to get metadata from HTTP endpoint for block %d"
	META_FAILED_TO_DECODE_BODY = "Failed to decode metadata body into a string for block %d"
	META_FAILED_SCALE_DECODE   = "Failed to scale decode metadata for block %d"
	META_STARTING              = "Metadata info is retrieved from the rpc endpoint"
	META_FINISHED              = "Metadata client successfully finished processing"
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
func (specVersionRangeList SpecVersionRangeList) getSpecVersionForBlock(blockHeight int) *SpecVersionRange {
	for idx, spec := range specVersionRangeList {
		if blockHeight >= spec.First && blockHeight <= spec.Last {
			return &specVersionRangeList[idx]
		}
	}
	messages.NewDictionaryMessage(
		messages.LOG_LEVEL_ERROR,
		messages.GetComponent(specVersionRangeList.getSpecVersionForBlock),
		nil,
		SPEC_VERSION_WRONG_BLOCK,
		blockHeight,
	).ConsoleLog()
	return nil
}
