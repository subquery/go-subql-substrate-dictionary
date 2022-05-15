package models

type EvmTransaction struct {
	Id          string //blockHeight-txId
	TxHash      string
	From        string
	To          string
	Func        string
	BlockHeight int
	Success     bool
}
