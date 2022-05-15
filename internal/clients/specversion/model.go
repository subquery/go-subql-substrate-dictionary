package specversion

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
