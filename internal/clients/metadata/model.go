package metadata

const (
	META_CLIENT_START        = "Metadata client starting"
	META_NO_TABLES           = "There are no tables in the current database"
	META_FAILED_DB           = "Failed to query the database"
	META_FAILED_JSON_MARSHAL = "Failed to marshal %s into a JSON object"
	META_FAILED_CHAIN_NAME   = "Failed to get chain name from http rpc endpoint"
	META_FAILED_CHAIN_DECODE = "Failed to decode chain name from json response"
)

type (
	RowCountEstimate struct {
		Estimate int    `json:"estimate"`
		Table    string `json:"table"`
	}
)
