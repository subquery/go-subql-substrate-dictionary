<div align="center">

# Subquery golang dictionary generator

### Subquery Dictionary Generator
</div>

Run alongside a Polkadot archive node to index all Events, SpecVersion, Extrinsic, Transaction and Logs data into PostgreSQL.

## Prerequisites
- Linux machine  ( !!! at this moment the project can be run only on Linux) 
 - Polkadot running archive node (it is recommended to first sync the node and disconnect it from the internet before running our tool)
 - PostgreSQL database
 - [.env](https://github.com/UnicornIdeas/substrate-archive/blob/master/go-dictionary/example.env) file with proper configuration (the file should be named .env and located in the root directory of the project)


## Install

The project was developed on linux and the following tutorial is for linux:

The rocksdb library was build with the following script:

    #!/bin/bash
    sudo apt-get install -y libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
    pushd /tmp
    git clone https://github.com/facebook/rocksdb.git && cd rocksdb
    git checkout v6.29.3
    export CXXFLAGS='-Wno-error=deprecated-copy -Wno-error=pessimizing-move -Wno-error=class-memaccess'
    make static_lib

After that we must set the flags for go:

    go env -w CGO_CFLAGS="-I/tmp/rocksdb/include"
    go env -w CGO_LDFLAGS="-L/tmp/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"

The next step is to start the polkadot node in archive mode:

    polkadot --name "Unicorn's node" --base-path *path_to_location_where_to_store_rocksdb_data* --chain polkadot --pruning archive --ws-max-out-buffer-capacity 1024

In the terminal that will start the project you must change the limit for opened files:

    ulimit -n 90000

To start program:

    go run src/setup/main.go
    go run src/indexer/main.go
    go run src/event_indexer/main.go


## Short presentation
We wanted our tool to be fast, easy to use and easy to deploy, so we separated each concern of the project into three main pillars. The first step will be represented by a setup step which should be run before the other two steps. This should be the fastest of the three and it's main concern is the downloading of the spec versions and of the metadata. The second and third steps can be interchanged, run at the same time, run on different machines, whatever you like. The second step downloads the events and the third the block info (logs, extrinsics and transactions) 

### Setup step
 Before running this step we need a config file containing all the spec versions known, each on a line.
 eg.: [example config file](https://github.com/UnicornIdeas/substrate-archive/blob/master/go-dictionary/spec_version_files/config)
 This file should be changed manually each time a new runtime version is added

This step is optimized based on some presumptions:

 1. there are no overlapping spec versions, this means that a spec version is valid only for a continuous range of blocks
 2. the spec versions are only in ascending order
 

Starting from these suppositions, we don't need to interrogate the Polkadot runtime to get the spec version for each block. In  return we did a modified binary search and queried  only the ends of the block range for the currently indexed blocks by archive node. 

This data is static, so we can save it in a file for future use (we save the spec version and the staring and ending blocks in a [.json](https://github.com/UnicornIdeas/substrate-archive/blob/master/go-dictionary/spec_version_files/ranges.json)  file for using with the second and third step). 

Initialising spec version downloader from config and connecting to the databases
![ ](https://raw.githubusercontent.com/UnicornIdeas/substrate-archive/master/go-dictionary/screenshots/spec_version_init.PNG)


Binary search spec version ranges

![enter image description here](https://raw.githubusercontent.com/UnicornIdeas/substrate-archive/master/go-dictionary/screenshots/spec_version_specs.PNG)

We got the spec versions for 4.3mil blocks in approx 50s

After getting the spec versions, we get the metadata specific for each spec version [metadata specific for each spec version](https://github.com/UnicornIdeas/substrate-archive/tree/master/go-dictionary/meta_files), again for future use

![enter image description here](https://raw.githubusercontent.com/UnicornIdeas/substrate-archive/master/go-dictionary/screenshots/metadata_save.PNG)

At the end, the spec versions are saved in the Postgres database

![enter image description here](https://raw.githubusercontent.com/UnicornIdeas/substrate-archive/master/go-dictionary/screenshots/spec_version_db_save.PNG)
![enter image description here](https://raw.githubusercontent.com/UnicornIdeas/substrate-archive/master/go-dictionary/screenshots/db_spec_version.PNG)

### Events step
This step downloads the block events interrogating the runtime rpc endpoint for each block (direct access of rocksdb is under research). The config files downloaded at the first step are used.

![enter image description here](https://raw.githubusercontent.com/UnicornIdeas/substrate-archive/master/go-dictionary/screenshots/events_start.PNG)
![enter image description here](https://raw.githubusercontent.com/UnicornIdeas/substrate-archive/master/go-dictionary/screenshots/events_finish.PNG)

approx **2 minutes** for downloading, processing and inserting in db **500000 block events** => approx **4200bps** for events 

![enter image description here](https://raw.githubusercontent.com/UnicornIdeas/substrate-archive/master/go-dictionary/screenshots/db_events.PNG)

### Extrinsics, logs and transactions step
The block data is directly queried from **RocksDB**

![enter image description here](https://raw.githubusercontent.com/UnicornIdeas/substrate-archive/master/go-dictionary/screenshots/500000%20blocks%20evm_logs+evm_transactions+exintrics%20test.png)

approx **1m42s** for **500000** blocks => approx **4900bps** for extrinsics, transactions and logs

The overall performance is over **4200bps**

This is not production ready, just a proof of concept
Possible improvements:
 - get the events from rocksdb (verify first that the effort is worth)
 - more routines behavior (always more routines)
