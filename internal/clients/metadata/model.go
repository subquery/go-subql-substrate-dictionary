package metadata

const (
	META_CLIENT_START        = "Metadata client starting"
	META_NO_TABLES           = "There are no tables in the current database"
	META_FAILED_DB           = "Failed to query the database"
	META_FAILED_JSON_MARSHAL = "Failed to marshal %s into a JSON object"
)

type (
	RowCountEstimate struct {
		Estimate int    `json:"estimate"`
		Table    string `json:"table"`
	}
)
