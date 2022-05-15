package models

type Extrinsic struct {
	Id          string //blockHeight-extrinsicId
	TxHash      string
	Module      string
	Call        string
	BlockHeight int
	Success     bool
	IsSigned    bool
}
