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

// FillLast fills the last block height for each spec version range
func (specVersionRangeList SpecVersionRangeList) FillLast(lastIndexedBlock int) {
	listLen := len(specVersionRangeList)
	for idx := 0; idx < listLen-1; idx++ {
		specVersionRangeList[idx].Last = specVersionRangeList[idx+1].First - 1
	}
	specVersionRangeList[listLen-1].Last = lastIndexedBlock
}

// GetSpecVersionForBlock receives a block height and returns it's spec version
func (specVersionRangeList SpecVersionRangeList) GetSpecVersionForBlock(blockHeight int) int {
	for idx, spec := range specVersionRangeList {
		if blockHeight >= spec.First && blockHeight <= spec.Last {
			specVNumber, _ := strconv.Atoi(specVersionRangeList[idx-1].SpecVersion)
			return specVNumber
		}
	}
	return -1
}
