package extrinsic

const (
	extrinsicCallModuleField  = "call_module"
	extrinsicFunctionField    = "call_module_function"
	extrinsicCallCodeField    = "call_code"
	extrinsicHashField        = "extrinsic_hash"
	extrinsicSignatureField   = "signature"
	extrinsicParamsField      = "params"
	extrinsicParamsInputField = "input"
	ethereumTransactType      = "3400"

	ethereumTransactModule = "Ethereum"

	// messages
	EXTRINSIC_DECODE_FAILED                 = "Failed to decode extrinsic for block %d"
	EXTRINSIC_FIELD_FAILED                  = "Failed to get extrinsic %s for block %d"
	EXTRINSIC_CLIENT_STARTING               = "Starting extrinsic client with %d workers"
	EXTRINSICS_NO_PREVIOUS_WORK             = "No previous extrinsic indexing was made"
	EXTRINSIC_FAILED_TO_RETRIEVE_LAST_BLOCK = "Failed to retrieve last block from previous indexing"
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

	ExtrinsicJob struct {
		BlockHeight    int
		BlockLookupKey []byte
	}
)
