package models

type EvmLog struct {
	Id          string //blockHeight-logId
	Address     string
	BlockHeight int
	Topics0     string
	Topics1     string //can be NULL
	Topics2     string //can be NULL
	Topics3     string //can be NULL
}
