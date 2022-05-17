package specversion

import "strconv"

type (
	SpecVersionRange struct {
		SpecVersion string `json:"spec_version"`
		First       int    `json:"first"` //first block for a spec version
		Last        int    `json:"last"`  //last block for a spec version
	}

	SpecVersionRangeList []SpecVersionRange
)

// FillFirst fills the first block height for each spec version range
func (specVersionRangeList SpecVersionRangeList) FillFirst() {
	for idx := 1; idx < len(specVersionRangeList); idx++ {
		specVersionRangeList[idx].First = specVersionRangeList[idx-1].Last + 1
	}
}

// GetSpecVersionForBlock receives a block height and returns it's spec version
func (specVersionRangeList SpecVersionRangeList) GetSpecVersionForBlock(blockHeight int) int {
	if blockHeight == 0 {
		return 0 //TODO: return first spec version for the chain
	}
	for idx, spec := range specVersionRangeList {
		if blockHeight == spec.First {
			specVNumber, _ := strconv.Atoi(specVersionRangeList[idx-1].SpecVersion)
			return specVNumber
		}
		//check only last version as the spec versions are in ascending order
		if blockHeight <= spec.Last {
			specVNumber, _ := strconv.Atoi(spec.SpecVersion)
			return specVNumber
		}
	}
	return -1
}
