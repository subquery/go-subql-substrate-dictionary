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
	PolkadotFirstSpecVersion int    `json:"polkadot_first_spec_version"`
	MoonbeamFirstSpecVersion int    `json:"moonbeam_first_spec_version"`
	HttpRpcEndpoint          string `json:"http_rpc_endpoint"`
	PolkadotTypesFile        string `json:"polkadot_types_file"`
}

type ClientData struct {
	Workers   int `json:"workers"`
	BatchSize int `json:"batch_size"`
}

type ClientsConfig struct {
	Events     ClientData `json:"events"`
	Extrinsics ClientData `json:"extrinsics"`
}

type RocksdbConfig struct {
	RocksdbPath          string `json:"rocksdb_path"`
	RocksdbSecondaryPath string `json:"rocksdb_secondary_path"`
}

type Config struct {
	ChainConfig    ChainConfig    `json:"chain_config"`
	RocksdbConfig  RocksdbConfig  `json:"rocksdb_config"`
	ClientsConfig  ClientsConfig  `json:"clients_config"`
	PostgresConfig PostgresConfig `json:"postgres_config"`
}
