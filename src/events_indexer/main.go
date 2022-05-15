package main

import (
	"encoding/hex"
	"fmt"
	"go-dictionary/db"
	"go-dictionary/internal"
	"go-dictionary/models"
	"go-dictionary/utils"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	scalecodec "github.com/itering/scale.go"
	"github.com/itering/scale.go/source"
	"github.com/itering/scale.go/types"
	"github.com/itering/scale.go/utiles"
	"github.com/itering/substrate-api-rpc/rpc"
	"github.com/joho/godotenv"
)

type EventsClient struct {
	conn          *websocket.Conn
	rocksdbClient internal.RockClient
	httpRpc       string
	eventsChan    chan *rpc.JsonRpcResult
	specRanges    *utils.SpecVersionRangeList
	dbClient      *db.PostgresClient
}

const (
	eventQuery = `{"id":%d,"method":"state_getStorage","params":["0x26aa394eea5630e07c48ae0c9558cef780d41e5e16056765bc8461851072c9d7","%s"],"jsonrpc":"2.0"}`
)

var maxWsBatch int

func main() {
	//LOAD env
	fmt.Println("* Loading env variables from .env...")
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println("Failed to load environment variables:", err)
		return
	}
	rpcEndpoint := os.Getenv("HTTP_RPC_ENDPOINT")
	wsEndpoint := os.Getenv("WS_RPC_ENDPOINT")
	rocksDbPath := os.Getenv("ROCKSDB_PATH")
	batchSize := os.Getenv("WS_EVENTS_BATCH_SIZE")
	maxWsBatch, err = strconv.Atoi(batchSize)
	if err != nil {
		fmt.Println("Wrong format for env WS_EVENTS_BATCH_SIZE:", err)
		return
	}
	eventsWorkers := os.Getenv("EVENTS_WORKERS")
	evWorkersNum, err := strconv.Atoi(eventsWorkers)
	if err != nil {
		fmt.Println("Wrong format for env EVENTS_WORKERS:", err)
		return
	}

	//LOAD ranges for spec versions
	fmt.Println("* Loading config info from files...")
	specVRanges, err := utils.GetSpecVersionsFromFile()
	if err != nil {
		fmt.Println("Failed to load configs from file")
		return
	}
	fmt.Println("Successfuly loaded configs from files")
	lastIndexedBlock := specVRanges[len(specVRanges)-1].Last

	//Connect to ws
	fmt.Println("* Connecting to ws rpc endpoint", wsEndpoint)
	wsConn, _, err := websocket.DefaultDialer.Dial(wsEndpoint, nil)
	if err != nil {
		fmt.Printf("Error connecting ws %d. Trying to reconnect...\n", err)
		return
	}
	fmt.Println("Connected to ws endpoint")

	//CONNECT to rocksdb
	fmt.Println("* Connecting to rocksdb at", rocksDbPath, "...")
	rdbClient, err := internal.OpenRocksdb(rocksDbPath)
	if err != nil {
		fmt.Println("Error opening rocksdb:", err)
		return
	}
	defer rdbClient.Close()
	fmt.Println("Rocksdb connected")

	//Register decoder custom types
	fmt.Println("* Registering decoder custom types...")
	c, err := ioutil.ReadFile(fmt.Sprintf("%s.json", "./network/polkadot"))
	if err != nil {
		fmt.Println("Failed to register types for network Polkadot:", err)
		return
	}
	types.RegCustomTypes(source.LoadTypeRegistry(c))
	fmt.Println("Types registered successfuly for Polkadot")

	fmt.Println("* Connecting to database...")
	dbClient, err := db.CreatePostgresPool()
	if err != nil {
		fmt.Println("Error connecting to db:", err)
		return
	}
	defer dbClient.Close()
	fmt.Println("Successfuly connected to Postgres")

	fmt.Println("* Starting database worker in a separate routine...")
	var dbWg sync.WaitGroup
	dbWg.Add(1)
	go dbClient.EventsWorker(&dbWg)

	evChan := make(chan *rpc.JsonRpcResult, 10000)

	evClient := EventsClient{
		conn:          wsConn,
		rocksdbClient: rdbClient,
		httpRpc:       rpcEndpoint,
		eventsChan:    evChan,
		specRanges:    &specVRanges,
		dbClient:      &dbClient,
	}

	var wg sync.WaitGroup
	ch := make(chan *[]byte, 1000)
	syncChannel := make(chan bool)
	t := time.Now()
	wg.Add(2)
	go evClient.readWs(&wg, lastIndexedBlock+1, syncChannel)
	go evClient.sendMessage(&wg, lastIndexedBlock+1, ch, syncChannel)
	for i := 0; i < evWorkersNum; i++ {
		wg.Add(1)
		go func() {
			evClient.processEvents(wg)
			defer wg.Done()
		}()
	}

	fmt.Println("* Getting raw events...")
	for i := 0; i <= lastIndexedBlock; i++ {
		hash, err := evClient.getBlockHash(i)
		if err != nil {
			fmt.Println("Failed to get hash for block", i)
			continue
		}
		msg := fmt.Sprintf(eventQuery, i, hash)
		bMsg := []byte(msg)
		ch <- &bMsg
	}

	wg.Wait()
	close(dbClient.WorkersChannels.EventsChannel)
	dbWg.Wait()
	fmt.Println(time.Now().Sub(t))
}

func (ev *EventsClient) sendMessage(wg *sync.WaitGroup, total int, ch chan *[]byte, synC chan bool) {
	defer wg.Done()
	c := 0
	for {
		msg := <-ch
		ev.conn.WriteMessage(1, *msg)
		c++
		if c%5000 == 0 {
			fmt.Println("Sent event requests for blocks up to", c)
			if c%maxWsBatch == 0 {
				<-synC
			}
		}
		if c == total {
			fmt.Println("Finished sending event requests for", total, "blocks")
			break
		}
	}
}

func (ev *EventsClient) readWs(wg *sync.WaitGroup, total int, synC chan bool) {
	defer wg.Done()
	c := 0
	for {
		v := &rpc.JsonRpcResult{}
		err := ev.conn.ReadJSON(v)
		if err != nil {
			fmt.Println("Failed to get events for a block:", err)
			return
		}
		ev.eventsChan <- v
		c++
		if c%5000 == 0 {
			fmt.Println("Received event messages for blocks up to", c)
		}
		if c%maxWsBatch == 0 {
			synC <- true
		}
		if c == total {
			close(ev.eventsChan)
			fmt.Println("Finished getting events for", total, "blocks")
			break
		}
	}
}

func (ev *EventsClient) getBlockHash(height int) (string, error) {
	lk, err := ev.rocksdbClient.GetLookupKeyForBlockHeight(height)
	if err != nil {
		return "", err
	}
	hash := hex.EncodeToString(lk[4:])
	return hash, nil
}

func (ev *EventsClient) processEvents(wg sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic:", r)
		}
		wg.Done()
	}()

	specsLen := len(*ev.specRanges)
	// specV => option mapping
	specVToDecoderOptions := make(map[int]types.ScaleDecoderOption, specsLen)
	for _, specV := range *ev.specRanges {
		specVToDecoderOptions[specV.SpecVersion] = types.ScaleDecoderOption{Metadata: specV.Meta, Spec: specV.SpecVersion}
	}

	for msg := range ev.eventsChan {
		rawEvent, err := msg.ToString()
		if err != nil {
			fmt.Println("Error processing events for block", msg.Id)
			continue
		}

		blockSpecV := ev.specRanges.GetBlockSpecVersion(msg.Id)
		option := specVToDecoderOptions[blockSpecV]
		e := scalecodec.EventsDecoder{}
		e.Init(types.ScaleBytes{Data: utiles.HexToBytes(rawEvent)}, &option)
		e.Process()
		eventsArray := e.Value.([]interface{})
		for _, evt := range eventsArray {
			event := models.Event{
				Id:          fmt.Sprintf("%d-%d", msg.Id, evt.(map[string]interface{})["event_idx"].(int)),
				Module:      strings.ToLower(evt.(map[string]interface{})["module_id"].(string)),
				Event:       evt.(map[string]interface{})["event_id"].(string),
				BlockHeight: msg.Id,
			}

			ev.dbClient.WorkersChannels.EventsChannel <- &event
		}
	}
}
