package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go-dictionary/db"
	"go-dictionary/internal"
	"go-dictionary/models"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/itering/substrate-api-rpc/rpc"
	"github.com/joho/godotenv"
)

type SpecVersionClient struct {
	specVersionConfigPath string
	metaFilePath          string
	knownSpecVersions     []int
	rocksdbClient         internal.RockClient
	httpEndpoint          string
}

type SpecVersionRange struct {
	SpecVersion int `json:"spec_version"`
	First       int `json:"first"` //first block for a spec version
	Last        int `json:"last"`  //last block for a spec version
}

type SpecVersionRangeList []SpecVersionRange

func main() {
	var wg sync.WaitGroup

	//LOAD env
	fmt.Println("* Loading env variables from .env...")
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println("Failed to load environment variables:", err)
		return
	}

	endpoint := os.Getenv("HTTP_RPC_ENDPOINT")
	fmt.Println("Loaded HTTP_RPC_ENDPOINT:", endpoint)
	metaFP := os.Getenv("METADATA_FILES_PARENT")
	fmt.Println("Loaded METADATA_FILES_PARENT:", metaFP)
	svConfigFile := os.Getenv("SPEC_VERSION_CONFIG_FILE")
	fmt.Println("Loaded SPEC_VERSION_CONFIG_FILE:", svConfigFile)
	svRangeFile := os.Getenv("SPEC_VERSION_RANGE_FILE")
	fmt.Println("Loaded SPEC_VERSION_RANGE_FILE:", svRangeFile)
	rocksDbPath := os.Getenv("ROCKSDB_PATH")
	fmt.Println("Loaded ROCKSDB_PATH:", rocksDbPath)

	//CONNECT to rocksdb
	fmt.Println("* Connecting to rocksdb at", rocksDbPath, "...")
	rdbClient, err := internal.OpenRocksdb(rocksDbPath)
	if err != nil {
		fmt.Println("Error opening rocksdb:", err)
		return
	}
	defer rdbClient.Close()
	fmt.Println("Rocksdb connected")

	//INIT spec version and metadata client
	fmt.Println("* Initialising spec version client...")
	fmt.Println("HTTP RPC endpoint:", endpoint)
	specVClient := SpecVersionClient{
		metaFilePath:          metaFP,
		specVersionConfigPath: svConfigFile,
		rocksdbClient:         rdbClient,
		httpEndpoint:          endpoint,
	}
	err = specVClient.init(endpoint)
	if err != nil {
		fmt.Println("Error initialising client:", err)
		return
	}
	fmt.Println("Spec version client initialised")

	//GET last synced block by the node we interrogate
	fmt.Println("* Getting last synced block...")
	lastBlock, err := specVClient.rocksdbClient.GetLastBlockSynced()
	if err != nil {
		fmt.Println("Error getting last finalized block:", err)
		return
	}
	fmt.Println("Last synced block:", lastBlock)

	fmt.Println("* Getting spec version for last indexed block...")
	lastSpec, err := specVClient.getSpecVersion(lastBlock)
	if err != nil {
		fmt.Println("Failed to get spec version for last indexed block:", err)
		return
	}
	fmt.Println("Last indexed block spec version:", lastSpec)

	specLen := 0
	specList := SpecVersionRangeList{}
	for i := 0; i < len(specVClient.knownSpecVersions); i++ {
		specList = append(specList, SpecVersionRange{SpecVersion: specVClient.knownSpecVersions[i]})
		specLen++
		if specVClient.knownSpecVersions[i] == lastSpec {
			break //exit the loop if we are on the last indexed spec version
		}
	}
	specList[0].First = 0                //spec version 0 starts from version 0
	specList[specLen-1].Last = lastBlock //the last block for the last biggest spec version indexed is the last known block

	fmt.Println("* Getting block ranges for spec versions...")
	t := time.Now()
	//start from 1 because we know that for spec version 0 the first block is 0
	for i := 1; i < specLen; i++ {
		blockNum, err := specVClient.getFirstBlockForSpecVersion(specVClient.knownSpecVersions[i], specList[i-1].First, lastBlock)
		if err != nil {
			fmt.Println("Failed getting first block for Spec Version:", specVClient.knownSpecVersions[i])
			return
		}
		specList[i].First = blockNum
		specList[i-1].Last = blockNum - 1
		fmt.Println("Spec version", specVClient.knownSpecVersions[i], "first block is", blockNum)
	}
	fmt.Println("Finished getting block ranges for spec versions in", time.Now().Sub(t))

	fmt.Println("* Writing ranges to file", svRangeFile)
	fileData, err := json.MarshalIndent(specList, "", " ")
	if err != nil {
		fmt.Println("Error marshaling data:", err)
		return
	}
	err = ioutil.WriteFile(svRangeFile, fileData, 0644)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}
	fmt.Println("Successfuly written block ranges to file")

	fmt.Println("* Creating directory for metadata files...")
	err = os.MkdirAll(metaFP, os.ModePerm)
	if err != nil {
		fmt.Println("Failed to create directory:", err)
		return
	}
	fmt.Println("Directory created successfuly")

	fmt.Println("* Getting metadata associated to spec versions and saving it in", metaFP, "...")
	for _, spec := range specList {
		wg.Add(1)
		go func(version, block int) {
			defer wg.Done()
			specVClient.generateMetaFileForBlock(version, block)
		}(spec.SpecVersion, spec.Last)
	}
	wg.Wait()
	fmt.Println("Finished downloading metadata")

	fmt.Println("* Connecting to database...")
	dbClient, err := db.CreatePostgresPool()
	if err != nil {
		fmt.Println("Error connecting to db:", err)
		return
	}
	defer dbClient.Close()
	fmt.Println("Successfuly connected to Postgres")

	fmt.Println("* Starting Postgres spec version worker...")
	if err != nil {
		fmt.Println("Wrong worker number formating:", err)
		return
	}

	var dbWg sync.WaitGroup
	dbWg.Add(1)
	go dbClient.SpecVersionsWorker(&dbWg)

	fmt.Println("* Inserting spec versions in db...")
	dbTime := time.Now()
	for _, spec := range specList {
		dbClient.WorkersChannels.SpecVersionsChannel <- &models.SpecVersion{Id: strconv.Itoa(spec.SpecVersion), BlockHeight: spec.First + 1} //+1 because the first block still uses the old specV
	}
	close(dbClient.WorkersChannels.SpecVersionsChannel)
	dbWg.Wait()
	fmt.Println("Finished inserting", specLen, "records in", time.Now().Sub(dbTime))
}

func (svc *SpecVersionClient) init(endpoint string) error {
	err := svc.loadConfigFromFile()
	if err != nil {
		return err
	}
	return nil
}

// load all the spec versions from a file
func (svc *SpecVersionClient) loadConfigFromFile() error {
	file, err := os.Open(svc.specVersionConfigPath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		s, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return err
		}
		svc.knownSpecVersions = append(svc.knownSpecVersions, s)
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

// search between start and end block heights the first node for a spec version;
// optimisation using modified binary search;
func (svc *SpecVersionClient) getFirstBlockForSpecVersion(specVersion int, start, end int) (int, error) {
	s := start
	e := end

	for {
		mid := (s + (e - 1)) / 2

		if e == s {
			return e, nil
		}

		if e-1 == s {
			spec, err := svc.getSpecVersion(s)
			if err != nil {
				return -1, err
			}

			if spec == specVersion {
				return s, nil
			}

			return e, nil
		}

		mSpec, err := svc.getSpecVersion(mid)
		if err != nil {
			return -1, err
		}

		if mSpec > specVersion {
			e = mid - 1
			continue
		}

		if mSpec == specVersion {
			beforeMSpec, err := svc.getSpecVersion(mid - 1)
			if err != nil {
				return -1, err
			}

			if beforeMSpec < specVersion {
				return mid, nil
			}

			e = mid - 1
			continue
		}

		if mSpec < specVersion {
			s = mid + 1
		}
	}
}

func (svc *SpecVersionClient) getSpecVersion(height int) (int, error) {
	hash, err := svc.getBlockHash(height)
	if err != nil {
		return -1, err
	}

	msg := fmt.Sprintf(`{"id":1,"method":"chain_getRuntimeVersion","params":["%s"],"jsonrpc":"2.0"}`, hash)
	reqBody := bytes.NewBuffer([]byte(msg))

	resp, err := http.Post(svc.httpEndpoint, "application/json", reqBody)
	if err != nil {
		return 0, err
	}

	v := &rpc.JsonRpcResult{}
	err = json.NewDecoder(resp.Body).Decode(&v)
	if err != nil {
		return 0, err
	}

	return v.ToRuntimeVersion().SpecVersion, nil
}

func (svc *SpecVersionClient) getBlockHash(height int) (string, error) {
	lk, err := svc.rocksdbClient.GetLookupKeyForBlockHeight(height)
	if err != nil {
		return "", err
	}
	hash := hex.EncodeToString(lk[4:])
	return hash, nil
}

//saves metadata to file for a specific file version
func (svc *SpecVersionClient) generateMetaFileForBlock(specVersion int, blockNumber int) {
	fmt.Println("Getting metadata for spec version", specVersion)

	hash, err := svc.getBlockHash(blockNumber)
	if err != nil {
		fmt.Printf("Error getting block hash: [err: %v] [specV: %d]\n", err, specVersion)
		return
	}

	reqBody := bytes.NewBuffer([]byte(rpc.StateGetMetadata(1, hash)))
	resp, err := http.Post(svc.httpEndpoint, "application/json", reqBody)
	if err != nil {
		fmt.Printf("Error getting metadata for spec version: [err: %v] [specV: %d]\n", err, specVersion)
		return
	}
	defer resp.Body.Close()

	fullPath := path.Join(svc.metaFilePath, fmt.Sprintf("%d", specVersion))
	f, err := os.Create(fullPath)
	if err != nil {
		fmt.Printf("Error creating meta file: [err: %v] [specV: %d]\n", err, specVersion)
		return
	}
	defer f.Close()

	metaBody := &rpc.JsonRpcResult{}
	json.NewDecoder(resp.Body).Decode(metaBody)

	metaString, err := metaBody.ToString()
	if err != nil {
		fmt.Printf("Error converting meta response to string: [err: %v] [specV: %d]\n", err, specVersion)
		return
	}

	_, err = f.WriteString(metaString)
	if err != nil {
		fmt.Printf("Error writing metadata to file: [err: %v] [specV: %d]\n", err, specVersion)
		return
	}

	fmt.Println("Successfuly save metadata to file for spec version", specVersion)
}
