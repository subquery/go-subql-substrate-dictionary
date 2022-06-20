package config

type PostgresConfig struct {
	User     string `json:"postgres_user"`
	Password string `json:"postgres_password"`
	Host     string `json:"postgres_host"`
	Port     string `json:"postgres_port"`
	Db       string `json:"postgres_db"`
	ConnPool int    `json:"postgres_conn_pool"`
}

type ChainConfig struct {
	HttpRpcEndpoint  string `json:"http_rpc_endpoint"`
	DecoderTypesFile string `json:"decoder_types_file"`
}

type ClientsConfig struct {
	EventsWorkers     int `json:"events_workers"`
	ExtrinsicsWorkers int `json:"extrinsics_workers"`
	BatchSize         int `json:"batch_size"`
}

type RocksdbConfig struct {
	RocksdbPath          string `json:"rocksdb_path"`
	RocksdbSecondaryPath string `json:"rocksdb_secondary_path"`
}

type Config struct {
	Evm            bool           `json:"evm"`
	ChainConfig    ChainConfig    `json:"chain_config"`
	RocksdbConfig  RocksdbConfig  `json:"rocksdb_config"`
	ClientsConfig  ClientsConfig  `json:"clients_config"`
	PostgresConfig PostgresConfig `json:"postgres_config"`
}
