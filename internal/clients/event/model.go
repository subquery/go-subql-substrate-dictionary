package event

const (
	eventTriePathKey = "26aa394eea5630e07c48ae0c9558cef780d41e5e16056765bc8461851072c9d7"
	headerTypeString = "Header"
)

type (
	Event struct {
		Id          string //blockheight-eventId
		Module      string
		Event       string
		BlockHeight int
	}

	eventJob struct {
		BlockHeight    int
		BlockLookupKey []byte
	}
)
