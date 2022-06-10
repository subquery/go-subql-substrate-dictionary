package clients

const (
	ORCHESTRATOR_START                                   = "Starting client orchestrator"
	ORCHESTRATOR_CLOSE                                   = "Closing client orchestrator"
	ORCHESTRATOR_START_EXTRINSIC_BATCH                   = "Starting extrinsic batch of size %d starting from block %d"
	ORCHESTRATOR_FINISH_EXTRINSIC_BATCH                  = "Extrinsic batch finished"
	ORCHESTRATOR_FAILED_TO_REGISTER_CUSTOM_DECODER_TYPES = "Failed to register types for network"
)

type (
	job struct {
		BlockHeight    int
		BlockLookupKey []byte
	}
)
