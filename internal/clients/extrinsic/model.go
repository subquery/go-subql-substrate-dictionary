package extrinsic

type Extrinsic struct {
	Id          string //blockHeight-extrinsicId
	TxHash      string
	Module      string
	Call        string
	BlockHeight int
	Success     bool
	IsSigned    bool
}

//TODO: change the structure if necessary
type ExtrinsicJob struct {
	BlockHeight    int
	BlockLookupKey []byte
}
