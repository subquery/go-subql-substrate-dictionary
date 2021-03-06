package event

const (
	triePathNibbleCount = 64
	headerTypeString    = "Header"
	topicsLen           = 4

	// negative block heights are used as commands
	updateExtrinsicCommand  = -1
	extrinsicSuccessCommand = -2

	// decoded event fields
	eventIdField       = "event_idx"
	eventModuleField   = "module_id"
	eventEventField    = "event_id"
	eventTypeField     = "type"
	eventParams        = "params"
	eventParamsAddress = "address"
	eventParamsTopics  = "topics"
	extrinsicIdField   = "extrinsic_idx"

	extrinsicSuccessCall = "ExtrinsicSuccess"
	extrinsicFailedCall  = "ExtrinsicFailed"
	evmLogCall           = "Log"
	ethereumExecutedCall = "Executed"
	ethereumModule       = "ethereum"
	evmModule            = "evm"

	// messages
	EVENT_CLIENT_STARTING                  = "Starting events client with %d workers"
	EVENT_STATE_ROOT_NOT_FOUND             = "State root hash was not found in the block header"
	EVENT_FIELD_FAILED                     = "Failed to get event field %s for an event at block height %d"
	EVENT_UNKNOWN_EXTRINSIC_SUCCESS_STATUS = "%s is not a valid extrinsic success status"
	EVENT_FAILED_TO_RETRIEVE_LAST_BLOCK    = "Failed to retrieve last event block from previous indexing"
	EVENT_NO_PREVIOUS_WORK                 = "No previous event indexing was made"
	EVENT_WRONG_UPDATE_NUMBER              = "%d extrinsic updates were sent, but only %d took effect"
)

var (
	eventTriePathHexNibbles = []byte{
		0x2, 0x6, 0xa, 0xa, 0x3, 0x9, 0x4, 0xe, 0xe, 0xa, 0x5, 0x6, 0x3, 0x0, 0xe, 0x0, 0x7, 0xc, 0x4, 0x8, 0xa, 0xe, 0x0, 0xc, 0x9, 0x5, 0x5, 0x8, 0xc, 0xe, 0xf, 0x7, 0x8, 0x0, 0xd, 0x4, 0x1, 0xe, 0x5, 0xe, 0x1, 0x6, 0x0, 0x5, 0x6, 0x7, 0x6, 0x5, 0xb, 0xc, 0x8, 0x4, 0x6, 0x1, 0x8, 0x5, 0x1, 0x0, 0x7, 0x2, 0xc, 0x9, 0xd, 0x7,
	}
	eventTriePathBytes = []byte{
		0x26, 0xaa, 0x39, 0x4e, 0xea, 0x56, 0x30, 0xe0, 0x7c, 0x48, 0xae, 0x0c, 0x95, 0x58, 0xce, 0xf7, 0x80, 0xd4, 0x1e, 0x5e, 0x16, 0x05, 0x67, 0x65, 0xbc, 0x84, 0x61, 0x85, 0x10, 0x72, 0xc9, 0xd7,
	}
	nibbleToZeroPaddedByte = map[byte]byte{
		0x0: 0x00,
		0x1: 0x10,
		0x2: 0x20,
		0x3: 0x30,
		0x4: 0x40,
		0x5: 0x50,
		0x6: 0x60,
		0x7: 0x70,
		0x8: 0x80,
		0x9: 0x90,
		0xa: 0xa0,
		0xb: 0xb0,
		0xc: 0xc0,
		0xd: 0xd0,
		0xe: 0xe0,
		0xf: 0xf0,
	}
)

type (
	Event struct {
		Id          string //blockheight-eventId
		Module      string
		Event       string
		BlockHeight int
	}

	UpdateExtrinsic struct {
		Id      string
		Success bool
	}

	UpdateEvmTransactions struct {
		Id      string
		TxHash  string
		From    string
		To      string
		Success bool
	}

	eventJob struct {
		BlockHeight    int
		BlockLookupKey []byte
		SpecVersion    int
	}

	EvmLog struct {
		Id          string //blockHeight-logId
		Address     string
		BlockHeight int
		Topics      []string
	}
)
