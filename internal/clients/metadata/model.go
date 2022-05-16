package metadata

import (
	"github.com/itering/scale.go/types"
	"github.com/itering/substrate-api-rpc/metadata"
)

type (
	DictionaryMetadata struct {
		Meta        *types.MetadataStruct
		MetaInstant *metadata.Instant
	}
)
