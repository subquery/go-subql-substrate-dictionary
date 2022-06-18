package clients

const (
	ORCHESTRATOR_INITIALIZING                    = "The dictionary orchestrator is initializing all the necessary services"
	ORCHESTRATOR_START                           = "Starting client orchestrator"
	ORCHESTRATOR_CLOSE                           = "Closing client orchestrator"
	ORCHESTRATOR_START_PROCESSING                = "Starting %s indexing starting from block %d"
	ORCHESTRATOR_FINISH_BATCH                    = "%s batch finished up to block %d"
	ORCHESTRATOR_FAILED_TO_READ_TYPES_FILE       = "Failed to read network custom types file"
	ORCHESTRATOR_BATCH_SIZE                      = "%s client batch size is %d blocks"
	ORCHESTRATOR_FAILED_TO_GET_LAST_SYNCED_BLOCK = "Failed to get last synced block, no finalized blocks in Rocksdb!"
)
