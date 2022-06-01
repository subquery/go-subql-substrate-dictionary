package event

const (
	eventTriePathKey = "26aa394eea5630e07c48ae0c9558cef780d41e5e16056765bc8461851072c9d7"
	headerTypeString = "Header"

	// negative block heights are used as commands
	updateExtrinsicCommand = -1

	// decoded event fields
	eventIdField     = "event_idx"
	eventModuleField = "module_id"
	eventEventField  = "event_id"

	extrinsicSuccess = "ExtrinsicSuccess"
	extrinsicFailed  = "ExtrinsicFailed"
)

var (
	notInsertableEvents map[string]struct{} = map[string]struct{}{
		"ExtrinsicSuccess": {},
		"ExtrinsicFailed":  {},
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

	eventJob struct {
		BlockHeight    int
		BlockLookupKey []byte
	}
)
