package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go-dictionary/db"
	"go-dictionary/internal"
	"go-dictionary/models"
	"go-dictionary/utils"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/itering/scale.go/source"
	"github.com/itering/scale.go/types"
	"github.com/joho/godotenv"
)

func main() {
	// FLAGS
	var configFilePath string
	flag.StringVar(&configFilePath, "c", "", "path to config file")
	flag.StringVar(&configFilePath, "configfile", "", "path to config file")
	flag.Parse()
	if configFilePath == "" {
		log.Println(`[INFO] No config file path specified with -c, --configfile. Default is "./config.json"`)
		configFilePath = "./config.json"
	}
	log.Println("[+] Loading .env variables!")
	config := models.Config{}
	configFile, err := os.Open(configFilePath)
	if err != nil {
		log.Println(err)
	}
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&config)
	fmt.Println(config)

	err = godotenv.Load(".env")
	if err != nil {
		log.Panic("[ERR] Failed to load environment variables: ", err)
		return
	}

	log.Println("[+] Initializing Postgres Database Pool")
	// Postgres database initialize
	postgresClient, err := db.CreatePostgresPool()
	if err != nil {
		log.Panic("[ERR] ", err, " - could not initialize postgres!")
	}
	//LOAD ranges for spec versions
	log.Println("[+] Loading config info from files...")
	specVRanges, err := utils.GetSpecVersionsFromFile()
	if err != nil {
		log.Panic("[ERR] Failed to load configs from file!")
		return
	}

	log.Println("[+] Initializing Rocksdb")
	rocksDbPath := os.Getenv("ROCKSDB_PATH")
	rc, err := internal.OpenRocksdb(rocksDbPath)
	if err != nil {
		log.Panic("[ERR] ", err, " - could not open Rocksdb!")
	}

	log.Println("[+] Initializing Pool Workers for Header and Body processing")
	// Pool Workers routines for Header and Body
	jobQueueHeader := internal.NewJobQueueHeader(1, postgresClient.WorkersChannels.EvmLogsChannel)
	jobQueueHeader.Start()

	jobQueueBody := internal.NewJobQueueBody(10, specVRanges, postgresClient.WorkersChannels.ExtrinsicsChannel, postgresClient.WorkersChannels.EvmTransactionsChannel)
	jobQueueBody.Start()

	//Register decoder custom types
	log.Println("[+] Registering decoder custom types...")
	c, err := ioutil.ReadFile(fmt.Sprintf("%s.json", "./network/polkadot"))
	if err != nil {
		log.Panic("[ERR] Failed to register types for network Polkadot: ", err)
		return
	}
	types.RegCustomTypes(source.LoadTypeRegistry(c))

	// Postgres Insert Workers
	var workersWG sync.WaitGroup
	workersWG.Add(1)
	go postgresClient.EvmLogsWorker(&workersWG)
	workersWG.Add(1)
	go postgresClient.EvmTransactionsWorker(&workersWG)
	workersWG.Add(1)
	go postgresClient.ExtrinsicsWorker(&workersWG)

	t := time.Now()
	rc.StartProcessing(jobQueueBody, jobQueueHeader, specVRanges[len(specVRanges)-1].Last)

	workersWG.Wait()

	log.Println("[INFO] All the processing took:", time.Since(t))
	postgresClient.Pool.Close()

	log.Println("[-] Exiting program...")
}
