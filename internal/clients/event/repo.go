package event

const (
	insertBufferInitialSize = 2000
	tableEventName          = "events"
	colId                   = "id"
	colModule               = "module"
	colEvent                = "event"
	colBlockHeight          = "block_height"
)

// insertEvent inserts an event in the database insertion buffer channel
func (repoClient *eventRepoClient) insertEvent(event *Event) {
	repoClient.dbChan <- event
}

func (repoClient *eventRepoClient) startDbWorker() {
	insertItems := make([][]interface{}, insertBufferInitialSize)
	workerCounter := 0
	counter := 0

	for event := range repoClient.dbChan {
		if event == nil {
			workerCounter++
			if workerCounter == repoClient.workersCount {
				repoClient.insertBatch(insertItems[:counter])
				counter = 0
				workerCounter = 0
				repoClient.batchFinishedChan <- struct{}{}
			}
			continue
		}

	}
}
