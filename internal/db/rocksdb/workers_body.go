package rocksdb

import (
	"encoding/hex"
	"go-dictionary/models"
	"go-dictionary/utils"
	"log"
	"strconv"
	"strings"
	"sync"

	scalecodec "github.com/itering/scale.go"
	"github.com/itering/scale.go/types"
	"github.com/itering/substrate-api-rpc"
	"github.com/mr-tron/base58"
	"golang.org/x/crypto/blake2b"
)

// BodyJob - structure for job processing
type BodyJob struct {
	BlockHeight    int
	BlockHash      string
	BlockLookupKey []byte
	BlockBody      []byte
	TransactionID  int
}

// WorkerBody - the worker threads that actually process the jobs for Body data
type WorkerBody struct {
	specVersionList        utils.SpecVersionRangeList
	extrinsicsChannel      chan *models.Extrinsic
	evmTransactionsChannel chan *models.EvmTransaction
	done                   sync.WaitGroup
	readyPool              chan chan BodyJob
	assignedJobQueue       chan BodyJob
	quit                   chan bool
}

// JobQueueBody - a queue for enqueueing jobs to be processed
type JobQueueBody struct {
	internalQueue          chan BodyJob
	readyPool              chan chan BodyJob
	workers                []*WorkerBody
	workersStopped         sync.WaitGroup
	extrinsicsChannel      chan *models.Extrinsic
	evmTransactionsChannel chan *models.EvmTransaction
}

// NewJobQueueBody - creates a new job queue
func NewJobQueueBody(maxWorkers int, specVersionList utils.SpecVersionRangeList, extrinsicsChannel chan *models.Extrinsic, evmTransactionsChannel chan *models.EvmTransaction) *JobQueueBody {
	workersStopped := sync.WaitGroup{}
	readyPool := make(chan chan BodyJob, maxWorkers)
	workers := make([]*WorkerBody, maxWorkers, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		workers[i] = NewWorkerBody(specVersionList, extrinsicsChannel, evmTransactionsChannel, readyPool, workersStopped)
	}
	return &JobQueueBody{
		internalQueue:          make(chan BodyJob),
		readyPool:              readyPool,
		workers:                workers,
		workersStopped:         workersStopped,
		extrinsicsChannel:      extrinsicsChannel,
		evmTransactionsChannel: evmTransactionsChannel,
	}
}

// Start - starts the worker routines and dispatcher routine
func (q *JobQueueBody) Start() {
	for i := 0; i < len(q.workers); i++ {
		q.workers[i].Start()
	}
	go q.dispatch()
}

func (q *JobQueueBody) dispatch() {
	for job := range q.internalQueue {
		workerChannel := <-q.readyPool // Check out an available worker
		workerChannel <- job           // Send the request to the channel
	}
	for i := 0; i < len(q.workers); i++ {
		q.workers[i].Stop()
	}
	q.workersStopped.Wait()
	close(q.extrinsicsChannel)
	close(q.evmTransactionsChannel)
	log.Println("[-] Closing Body Pool...")
}

// Submit - adds a new BodyJob to be processed
func (q *JobQueueBody) Submit(job *BodyJob) {
	q.internalQueue <- *job
}

// NewWorkerBody - creates a new workerBody
func NewWorkerBody(specVersionList utils.SpecVersionRangeList, extrinsicsChannel chan *models.Extrinsic, evmTransactionsChannel chan *models.EvmTransaction, readyPool chan chan BodyJob, done sync.WaitGroup) *WorkerBody {
	return &WorkerBody{
		specVersionList:        specVersionList,
		extrinsicsChannel:      extrinsicsChannel,
		evmTransactionsChannel: evmTransactionsChannel,
		done:                   done,
		readyPool:              readyPool,
		assignedJobQueue:       make(chan BodyJob),
		quit:                   make(chan bool),
	}
}

// Start - begins the job processing loop for the workerBody
func (w *WorkerBody) Start() {
	go func() {
		w.done.Add(1)
		for {
			w.readyPool <- w.assignedJobQueue // check the job queue in
			select {
			case job := <-w.assignedJobQueue: // see if anything has been assigned to the queue
				job.ProcessBody(w.extrinsicsChannel, w.evmTransactionsChannel, w.specVersionList)
			case <-w.quit:
				w.done.Done()
				return
			}
		}
	}()
}

// Stop - stops the workerBody
func (w *WorkerBody) Stop() {
	w.quit <- true
}

func EncodeAddressId(txHash string) string {
	checksumBytes, _ := hex.DecodeString(SS58PRE + polkaAddressPrefix + txHash)
	checksum := blake2b.Sum512(checksumBytes)
	checksumEnd := hex.EncodeToString(checksum[:2])
	finalBytes, _ := hex.DecodeString(polkaAddressPrefix + txHash + checksumEnd)
	return base58.Encode(finalBytes)
}

func (job *BodyJob) ProcessBody(extrinsicsChannel chan *models.Extrinsic, evmTransactionsChannel chan *models.EvmTransaction, specVersionList utils.SpecVersionRangeList) {
	bodyDecoder := types.ScaleDecoder{}
	bodyDecoder.Init(types.ScaleBytes{Data: job.BlockBody}, nil)
	decodedBody := bodyDecoder.ProcessAndUpdateData("Vec<Bytes>")

	if bodyList, ok := decodedBody.([]interface{}); ok {
		extrinsics := []string{}
		for bodyPos := range bodyList {
			if bl, ok := bodyList[bodyPos].(string); ok {
				extrinsics = append(extrinsics, bl)
			}
		}
		specV, instant := specVersionList.GetBlockSpecVersionAndInstant(job.BlockHeight)

		decodedExtrinsics, err := substrate.DecodeExtrinsic(extrinsics, instant, specV)
		if err == nil {
			job.TransactionID = 0
			for decodedExtrinsicPos := range decodedExtrinsics {
				extrinsicsResult := models.Extrinsic{}
				extrinsicsResult.Id = strconv.Itoa(job.BlockHeight) + "-" + strconv.Itoa(decodedExtrinsicPos)
				if callModule, ok := decodedExtrinsics[decodedExtrinsicPos]["call_module"].(string); ok {
					extrinsicsResult.Module = strings.ToLower(callModule)
				}
				if callModuleFunction, ok := decodedExtrinsics[decodedExtrinsicPos]["call_module_function"].(string); ok {
					extrinsicsResult.Call = callModuleFunction
				}
				extrinsicsResult.BlockHeight = job.BlockHeight
				extrinsicsResult.Success = true

				if txHash, ok := decodedExtrinsics[decodedExtrinsicPos]["extrinsic_hash"].(string); ok {
					extrinsicsResult.TxHash = txHash
				}
				if _, ok := decodedExtrinsics[decodedExtrinsicPos]["signature"]; ok {
					extrinsicsResult.IsSigned = true
				}
				extrinsicsChannel <- &extrinsicsResult

				// 	// if decodedExtrinsics[decodedExtrinsicPos]["version_info"] == "84" {
				// 	if extrinsicsResult.Module == "balances" {
				// 		// log.Println(extrinsicsResult.Module, extrinsicsResult.Call, decodedExtrinsics[decodedExtrinsicPos]["call_code"], job.BlockHeight)
				// 		// job.ExtrinsicsModuleBalances(&extrinsicsResult, decodedExtrinsics[decodedExtrinsicPos], evmTransactionsChannel)
				// 	}
				// 	if extrinsicsResult.Module == "sudo" {
				// 		// log.Println(extrinsicsResult.Module, extrinsicsResult.Call, decodedExtrinsics[decodedExtrinsicPos]["call_code"], job.BlockHeight)
				// 	}
				// 	if extrinsicsResult.Module == "utility" {
				// 		log.Println(extrinsicsResult.Module, extrinsicsResult.Call, decodedExtrinsics[decodedExtrinsicPos], job.BlockHeight)
				// 	}
				// 	// if extrinsicsResult.Module == "sudo" && extrinsicsResult.Call == "sudo_unchecked_weight" { //} || extrinsicsResult.Module == "recovery" || extrinsicsResult.Module == "proxy" || extrinsicsResult.Module == "multisig" {
				// 	// 	log.Println(job.BlockHeight, extrinsicsResult.Module, extrinsicsResult.Call)
				// 	// 	log.Println(decodedExtrinsics[decodedExtrinsicPos]["params"].([]scalecodec.ExtrinsicParam)[0].Name)
				// 	// 	log.Println(decodedExtrinsics[decodedExtrinsicPos]["params"].([]scalecodec.ExtrinsicParam)[0].Type)
				// 	// 	log.Println(decodedExtrinsics[decodedExtrinsicPos]["params"].([]scalecodec.ExtrinsicParam)[0].TypeName)
				// 	// 	// log.Println(decodedExtrinsics[decodedExtrinsicPos]["params"].([]scalecodec.ExtrinsicParam))
				// 	// }
				// 	// }
				// 	if _, ok := decodedExtrinsics[decodedExtrinsicPos]["params"].([]scalecodec.ExtrinsicParam); ok {

				// 		// if extrinsicsResult.Module == "utility" {
				// 		// 	job.ProcessUtilityTransaction(transactions, extrinsicsResult.TxHash, evmTransactionsChannel)
				// 		// } else if extrinsicsResult.Module == "sudo" {
				// 		// 	job.ProcessBalances(transactions, extrinsicsResult.TxHash, evmTransactionsChannel)
				// 		// }
				// 	}
			}
		} else {
			log.Println("[ERR] ", err, "- could not decode extrinsic!", specV, job.BlockHeight)
		}
	}
}

func (job *BodyJob) ExtrinsicsModuleBalances(extrinsic *models.Extrinsic, decodedExtrinsics map[string]interface{}, evmTransactionsChannel chan *models.EvmTransaction) {
	switch extrinsic.Call {
	case "transfer":
		log.Println(decodedExtrinsics, job.BlockHeight)
	case "force_transfer":
		log.Println("balances -> forcetransfer")
	case "transferall":
		log.Println("balances -> transferall")
	case "transferkeepalive":
		log.Println("balances -> transferkeepalive")
	default:
		log.Println(extrinsic.Call)
	}
}

func (job *BodyJob) ProcessBalances(transactions []scalecodec.ExtrinsicParam, txHash string, evmTransactionsChannel chan *models.EvmTransaction) {
	for transactionPos := range transactions {
		if transactions[transactionPos].Name == "call" {
			if value, ok := transactions[transactionPos].Value.(map[string]interface{}); ok {
				if callModule, ok := value["call_module"].(string); ok && callModule == "Balances" {
					temporaryTransaction := job.ProcessBalancesTransaction(transactions[transactionPos], txHash)
					evmTransactionsChannel <- &temporaryTransaction
				}
			}
		}
	}
}

func (job *BodyJob) ProcessBalancesTransaction(transaction scalecodec.ExtrinsicParam, txHash string) models.EvmTransaction {
	tempTransaction := models.EvmTransaction{}
	if transactionData, ok := transaction.Value.(map[string]interface{}); ok {
		tempTransaction.Id = strconv.Itoa(job.BlockHeight) + "-" + strconv.Itoa(job.TransactionID)
		tempTransaction.TxHash = txHash
		if params, ok := transactionData["params"].([]types.ExtrinsicParam); ok {
			for paramPos := range params {
				if params[paramPos].Name == "source" {
					if source, ok := params[paramPos].Value.(string); ok {
						tempTransaction.From = EncodeAddressId(source)
					}
				}
				if params[paramPos].Name == "dest" {
					if dest, ok := params[paramPos].Value.(string); ok {
						tempTransaction.To = EncodeAddressId(dest)
					}
				}
			}
			if callModule, ok := transactionData["call_module"].(string); ok {
				tempTransaction.Func = callModule
			}
			tempTransaction.BlockHeight = job.BlockHeight
			tempTransaction.Success = true
		}
	}
	job.TransactionID++
	return tempTransaction
}

func (job *BodyJob) ExtractInfoFromBalances(transactionDataRaw interface{}, txHash string) models.EvmTransaction {
	tempTransaction := models.EvmTransaction{}
	if transactionData, ok := transactionDataRaw.(map[string]interface{}); ok {
		if transactionModule, ok := transactionData["call_module"].(string); ok && transactionModule == "Balances" {
			tempTransaction.Id = strconv.Itoa(job.BlockHeight) + "-" + strconv.Itoa(job.TransactionID)
			tempTransaction.TxHash = txHash
			tempTransaction.Func = transactionModule
			if transactionParams, ok := transactionData["params"].([]types.ExtrinsicParam); ok {
				for tpPos := range transactionParams {
					if transactionParams[tpPos].Name == "source" {
						if source, ok := transactionParams[tpPos].Value.(string); ok {
							tempTransaction.From = EncodeAddressId(source)
						}
					}
					if transactionParams[tpPos].Name == "dest" {
						if dest, ok := transactionParams[tpPos].Value.(string); ok {
							tempTransaction.To = EncodeAddressId(dest)
						}
					}
					tempTransaction.BlockHeight = job.BlockHeight
					tempTransaction.Success = true
				}
			}
		}
	}
	job.TransactionID++
	return tempTransaction
}

func (job *BodyJob) ProcessUtilityTransaction(transactions []scalecodec.ExtrinsicParam, txHash string, evmTransactionsChannel chan *models.EvmTransaction) {
	for transactionPos := range transactions {
		if transactions[transactionPos].Name == "calls" {
			if transactionValue, ok := transactions[transactionPos].Value.([]interface{}); ok {
				for tvPos := range transactionValue {
					if params, ok := transactionValue[tvPos].(map[string]interface{})["params"].([]types.ExtrinsicParam); ok {
						for paramPos := range params {
							if params[paramPos].Name == "call" {
								if transactionModule, ok := params[paramPos].Value.(map[string]interface{})["call_module"]; ok && transactionModule == "Balances" {
									temporaryTransaction := job.ExtractInfoFromBalances(params[paramPos].Value, txHash)
									evmTransactionsChannel <- &temporaryTransaction
								}
							}
						}
					}
				}
			}
		}
	}
}
