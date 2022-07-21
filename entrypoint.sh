#!/bin/bash
set -eux

function configureFromENV () {

    if [[ (-v DB_PORT) && (-v DB_DATABASE) && (-v DB_HOST) && (-v DB_PASS) && (-v DB_USER) && (-v DB_SCHEMA) ]]; then
        cat /config.json | jq ".postgres_config.postgres_user=\"$DB_USER\" | .postgres_config.postgres_password=\"$DB_PASS\" | .postgres_config.postgres_host=\"$DB_HOST\" | .postgres_config.postgres_port=\"$DB_PORT\" | .postgres_config.postgres_db=\"$DB_DATABASE\" | .postgres_config.postgres_schema=\"$DB_SCHEMA\"" > config.json.tmp && mv config.json.tmp config.json
    fi



    if [[ (-v CFG_EVM) ]]; then
        cat /config.json | jq ".evm=$CFG_EVM" > /config.json.tmp && mv config.json.tmp config.json

    fi

    if [[ (-v CFG_RKDB_PATH) ]]; then
        cat /config.json | jq ".rocksdb_config.rocksdb_path=\"$CFG_RKDB_PATH\"" > /config.json.tmp && mv config.json.tmp config.json
    fi

    if [[ (-v CFG_RKDB_PATH_SEC) ]]; then
        cat /config.json | jq ".rocksdb_config.rocksdb_secondary_path=\"$CFG_RKDB_PATH_SEC\"" > /config.json.tmp && mv config.json.tmp config.json
    fi

    if [[ (-v CFG_HTTP_RPC) ]]; then
        cat /config.json | jq ".chain_config.http_rpc_endpoint=\"$CFG_HTTP_RPC\"" > /config.json.tmp && mv config.json.tmp config.json
    fi

    if [[ (-v CFG_CHAIN_FML) ]]; then
        if [ "$CFG_CHAIN_FML" == "polkadot" ]; then
            CFG_CHAIN_FML="/polkadot.json"
        fi
        if [ "$CFG_CHAIN_FML" == "moonbeam" ]; then
            CFG_CHAIN_FML="/moonbeam.json"
        fi
        cat /config.json | jq ".chain_config.decoder_types_file=\"$CFG_CHAIN_FML\"" > /config.json.tmp && mv config.json.tmp config.json
    fi

}

configureFromENV

while date; do sleep 60; done &

/go-subql-substrate-dictionary "$@" 2>&1