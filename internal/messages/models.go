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
	CONFIG_FAILED_TO_OPEN_FILE      = "Failed to open configuration file %s"
	CONFIG_WRONG_FILE_FORMAT        = "The configuration files isn't in the right format"

	// rocksdb messages
	ROCKSDB_CONNECTING                      = "Connecting to rocksdb instance at %s"
	ROCKSDB_FAILED_TO_LIST_COLUMN_FAMILIES  = "Failed to list rocksdb column families"
	ROCKSDB_FAILED_TO_CONNECT               = "Failed to connect to rocksdb instance at %s"
	ROCKSDB_FAILED_LOOKUP_KEY               = "Failed to get lookup key for block %d"
	ROCKSDB_CONNECTED                       = "Successfully connected to rocksdb instance"
	ROCKSDB_FAILED_TO_GET_LAST_SYNCED_BLOCK = "Failed to get last synced block"
	ROCKSDB_FAILED_BODY                     = "Failed to retrieve block body from rocksdb"
	ROCKSDB_FAILED_HEADER                   = "Failed to retrieve block header from rocksdb"
	ROCKSDB_FAILED_TO_UPDATE_SECONDARY      = "Failed to synchronize secondary rocksdb instance with primary"
	ROCKSDB_FAILED_TRIE_NODE_DB             = "Failed to get trie node from rocksdb"
	ROCKSDB_FAILED_GENESIS                  = "Failed to get genesis hash from rocksdb instance"

	// postgres messages
	POSTGRES_CONNECTING                        = "Connecting to postgres database at '%s'"
	POSTGRES_CONNECTED                         = "Successfully connected to postgres instance"
	POSTGRES_FAILED_TO_PARSE_CONNECTION_STRING = "Failed to parse postgres connection string"
	POSTGRES_FAILED_TO_CONNECT                 = "Failed to connect to postgres database"
	POSTGRES_FAILED_TO_PING                    = "Failed to ping postgres database instance"
	POSTGRES_FAILED_TO_START_TRANSACTION       = "Failed to start postgres transaction"
	POSTGRES_FAILED_TO_EXECUTE_UPDATE          = "Failed to execute update statement"
	POSTGRES_FAILED_TO_INSERT                  = "Failed to execute insert statement"
	POSTGRES_FAILED_TO_COPY_FROM               = "Postgres failed to copy from rows"
	POSTGRES_WRONG_NUMBER_OF_COPIED_ROWS       = "Postgres copied %d rows out of %d"
	POSTGRES_FAILED_TO_COMMIT_TX               = "Failed to commit postgres transaction"
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
