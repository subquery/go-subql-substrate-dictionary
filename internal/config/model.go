package config

type PostgresConfig struct {
	User     string `json:"postgres_user"`
	Password string `json:"postgres_password"`
	Host     string `json:"postgres_host"`
	Port     string `json:"postgres_port"`
	Db       string `json:"postgres_db"`
	ConnPool int    `json:"postgres_conn_pool"`
}

type ConnectionConfig struct {
	HttpRpcEndpoint string `json:"http_rpc_endpoint"`
	WsRpcEndpoint   string `json:"ws_rpc_endpoint"`
	EventsBatchSize int    `json:"ws_events_batch_size"`
}

type ChainConfig struct {
	FirstSpecVersion int `json:"starting_spec_version"`
}

type WorkersConfig struct {
	SpecVersionWorkers int `json:"spec_version_workers"`
	EventsWorkers      int `json:"events_workers"`
}

type RocksdbConfig struct {
	RocksdbPath          string `json:"rocksdb_path"`
	RocksdbSecondaryPath string `json:"rocksdb_secondary_path"`
}

type Config struct {
	ConnectionConfig  ConnectionConfig `json:"connection_config"`
	RocksdbConfig     RocksdbConfig    `json:"rocksdb_config"`
	WorkersConfig     WorkersConfig    `json:"workers_config"`
	SpecVersionConfig ChainConfig      `json:"chain_config"`
	PostgresConfig    PostgresConfig   `json:"postgres_config"`
}
