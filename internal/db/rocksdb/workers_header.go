package rocksdb

import (
	"go-dictionary/models"
	"log"
	"strconv"
	"sync"

	"github.com/itering/scale.go/types"
)

// Job - structure for job processing
type HeaderJob struct {
	BlockHeight    int
	BlockHash      string
	BlockLookupKey []byte
	BlockHeader    []byte
}

// Worker - the worker threads that actually process the jobs
type WorkerHeader struct {
	evmLogsChannel   chan *models.EvmLog
	readyPool        chan chan HeaderJob
	assignedJobQueue chan HeaderJob
	done             sync.WaitGroup
	quit             chan bool
}

// JobQueue - a queue for enqueueing jobs to be processed
type JobQueueHeader struct {
	internalQueue  chan HeaderJob
	readyPool      chan chan HeaderJob
	workers        []*WorkerHeader
	workersStopped sync.WaitGroup
	evmLogsChannel chan *models.EvmLog
}

// NewJobQueue - creates a new job queue
func NewJobQueueHeader(maxWorkers int, evmLogsChannel chan *models.EvmLog) *JobQueueHeader {
	workersStopped := sync.WaitGroup{}
	readyPool := make(chan chan HeaderJob, maxWorkers)
	workers := make([]*WorkerHeader, maxWorkers, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		workers[i] = NewWorkerHeader(evmLogsChannel, readyPool, workersStopped)
	}
	return &JobQueueHeader{
		internalQueue:  make(chan HeaderJob),
		readyPool:      readyPool,
		workers:        workers,
		workersStopped: workersStopped,
		evmLogsChannel: evmLogsChannel,
	}
}

// Start - starts the worker routines and dispatcher routine
func (q *JobQueueHeader) Start() {
	for i := 0; i < len(q.workers); i++ {
		q.workers[i].Start()
	}
	go q.dispatch()
}

func (q *JobQueueHeader) dispatch() {
	for job := range q.internalQueue {
		workerChannel := <-q.readyPool // Check out an available worker
		workerChannel <- job           // Send the request to the channel
	}
	for i := 0; i < len(q.workers); i++ {
		q.workers[i].Stop()
	}
	q.workersStopped.Wait()
	close(q.evmLogsChannel)
	log.Println("[-] Closing Header Pool...")
}

// Submit - adds a new job to be processed
func (q *JobQueueHeader) Submit(job *HeaderJob) {
	q.internalQueue <- *job
}

// NewWorker - creates a new worker
func NewWorkerHeader(evmLogsChannel chan *models.EvmLog, readyPool chan chan HeaderJob, done sync.WaitGroup) *WorkerHeader {
	return &WorkerHeader{
		evmLogsChannel:   evmLogsChannel,
		done:             done,
		readyPool:        readyPool,
		assignedJobQueue: make(chan HeaderJob),
		quit:             make(chan bool),
	}
}

// Start - begins the job processing loop for the WorkerHeader
func (w *WorkerHeader) Start() {
	go func() {
		w.done.Add(1)
		for {
			w.readyPool <- w.assignedJobQueue // check the job queue in
			select {
			case job := <-w.assignedJobQueue: // see if anything has been assigned to the queue
				job.ProcessHeader(w.evmLogsChannel)
			case <-w.quit:
				w.done.Done()
				return
			}
		}
	}()
}

// Stop - stops the workerBody
func (w *WorkerHeader) Stop() {
	w.quit <- true
}

// Processing function
func (job *HeaderJob) ProcessHeader(evmLogsChannel chan *models.EvmLog) {
	headerDecoder := types.ScaleDecoder{}
	headerDecoder.Init(types.ScaleBytes{Data: job.BlockHeader}, nil)
	decodedHeader := headerDecoder.ProcessAndUpdateData("Header")

	if logs, ok := decodedHeader.(map[string]interface{})["digest"].(map[string]interface{})["logs"].([]interface{}); ok {
		for i := range logs {
			evmLog := models.EvmLog{}
			evmLog.Id = strconv.Itoa(job.BlockHeight) + "-" + strconv.Itoa(i)
			evmLog.BlockHeight = job.BlockHeight
			evmLogsChannel <- &evmLog
		}
	}
}
