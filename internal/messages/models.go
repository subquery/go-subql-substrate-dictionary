package messages

import "runtime"

type DictionaryLogLevel string

var (
	reset  = "\033[0m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	blue   = "\033[34m"
	purple = "\033[35m"
	cyan   = "\033[36m"
	gray   = "\033[37m"
	white  = "\033[97m"

	// generics
	FAILED_ATOI           = "Failed to convert string to integer"
	FAILED_TYPE_ASSERTION = "Failed type assertion"

	// configuration info messages
	CONFIG_NO_CUSTOM_PATH_SPECIFIED = "No config file path specified with --c, --configfile. Using default path."
	CONFIG_STARTED_LOADING          = "The dictionary configuration is loaded from %s"
	CONFIG_FINISHED_LOADING         = "The dictionary configuration successfully loaded"

	// rocksdb messages
	ROCKSDB_CONNECTING                      = "Connecting to rocksdb instance at %s"
	ROCKSDB_FAILED_TO_LIST_COLUMN_FAMILIES  = "Failed to list rocksdb column families"
	ROCKSDB_FAILED_TO_CONNECT               = "Failed to connect to rocksdb instance at %s"
	ROCKSDB_FAILED_LOOKUP_KEY               = "Failed to get lookup key for block %d"
	ROCKSDB_CONNECTED                       = "Successfully connected to rocksdb instance"
	ROCKSDB_FAILED_TO_GET_LAST_SYNCED_BLOCK = "Failed to get last synced block"
	ROCKSDB_FAILED_BODY                     = "Failed to retrieve block body from rocksdb"

	// postgres
	POSTGRES_CONNECTING                        = "Connecting to postgres database using '%s'"
	POSTGRES_CONNECTED                         = "Successfully connected to postgres instance"
	POSTGRES_FAILED_TO_PARSE_CONNECTION_STRING = "Failed to parse postgres connection string"
	POSTGRES_FAILED_TO_CONNECT                 = "Failed to connect to postgres database"
	POSTGRES_FAILED_TO_PING                    = "Failed to ping postgres database instance"
	POSTGRES_FAILED_TO_START_TRANSACTION       = "Failed to start postgres transaction"
	POSTGRES_FAILED_TO_EXECUTE_UPDATE          = "Failed to execute update statement"
	POSTGRES_FAILED_TO_COPY_FROM               = "Postgres failed to copy from rows"
	POSTGRES_WRONG_NUMBER_OF_COPIED_ROWS       = "Postgres copied %d rows out of %d"
	POSTGRES_FAILED_TO_COMMIT_TX               = "Failed to commit postgres transaction"

	// spec version
	SPEC_VERSION_FAILED_POST_MESSAGE  = "Failed to get spec version from HTTP endpoint for block %d"
	SPEC_VERSION_FAILED_TO_DECODE     = "Failed to decode spec version for block %d"
	SPEC_VERSION_FAILED_DB_LAST_BLOCK = "Failed to get last spec version block from db"
	SPEC_VERSION_FAILED_DB            = "Failed to get spec version info from db"
	SPEC_VERSION_RETRIEVED            = "Last block height for spec version %d is %d"
	SPEC_VERSION_DB_INSERT            = "Inserting spec verion data in db starting from block %d"
	SPEC_VERSION_DB_SUCCESS           = "Successfully inserted spec version info in db"
	SPEC_VERSION_UP_TO_DATE           = "Spec versions saved in db are up to date"
	SPEC_VERSION_NO_PREVIOUS_WORK     = "No spec version info was saved from previous executions"
	SPEC_VERSION_RECOVERED            = "Spec version recovered from last run"

	// metadata
	META_FAILED_POST_MESSAGE   = "Failed to get metadata from HTTP endpoint for block %d"
	META_FAILED_TO_DECODE_BODY = "Failed to decode metadata body into a string for block %d"
	META_FAILED_SCALE_DECODE   = "Failed to scale decode metadata for block %d"
	META_STARTING              = "Metadata info is retrieved from the rpc endpoint"
	META_FINISHED              = "Metadata client successfully finished processing"

	// extrinsic
	EXTRINSIC_DECODE_FAILED                 = "Failed to decode extrinsic for block %d"
	EXTRINSIC_FIELD_FAILED                  = "Failed to get extrinsic %s for block %d"
	EXTRINSIC_CLIENT_STARTING               = "Starting extrinsic client with %d workers"
	EXTRINSICS_NO_PREVIOUS_WORK             = "No previous extrinsic indexing was made"
	EXTRINSIC_FAILED_TO_RETRIEVE_LAST_BLOCK = "Failed to retrieve last block from previous indexing"

	// event
	EVENT_FAILED_TRIE_NODE_DB = "Failed to get trie node from rocksdb"

	// orchestrator
	ORCHESTRATOR_START                  = "Starting client orchestrator"
	ORCHESTRATOR_CLOSE                  = "Closing client orchestrator"
	ORCHESTRATOR_START_EXTRINSIC_BATCH  = "Starting extrinsic batch of size %d starting from block %d"
	ORCHESTRATOR_FINISH_EXTRINSIC_BATCH = "Extrinsic batch finished"
)

const (
	// log levels used by substrate dictionary
	LOG_LEVEL_INFO    DictionaryLogLevel = "INFO"
	LOG_LEVEL_ERROR   DictionaryLogLevel = "ERROR"
	LOG_LEVEL_WARNING DictionaryLogLevel = "WARNING"
	LOG_LEVEL_SUCCESS DictionaryLogLevel = "SUCCESS"
)

func init() {
	if runtime.GOOS == "windows" {
		reset = ""
		red = ""
		green = ""
		yellow = ""
		blue = ""
		purple = ""
		cyan = ""
		gray = ""
		white = ""
	}
}

type DictionaryMessage struct {
	LogLevel       DictionaryLogLevel
	Component      string
	Error          error
	FormatString   string
	AdditionalInfo []interface{}
}
