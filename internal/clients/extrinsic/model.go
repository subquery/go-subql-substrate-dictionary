package extrinsic

const (
	extrinsicCallModuleField = "call_module"
	extrinsicFunctionField   = "call_module_function"
	extrinsicHashField       = "extrinsic_hash"
	extrinsicSignatureField  = "signature"
)

type (
	Extrinsic struct {
		Id          string //blockHeight-extrinsicId
		TxHash      string
		Module      string
		Call        string
		BlockHeight int
		Success     bool
		IsSigned    bool
	}

	//TODO: change the structure if necessary
	ExtrinsicJob struct {
		BlockHeight    int
		BlockLookupKey []byte
	}
)
