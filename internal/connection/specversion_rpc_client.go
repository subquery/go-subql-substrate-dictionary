package connection

import (
	"sync"

	"github.com/itering/substrate-api-rpc/rpc"
)

type SpecversionClient struct {
	wsclient *WsClient
	receiver chan *rpc.JsonRpcResult
	caller   sync.Map
}

func NewSpecVersionClient(endpoint string) (*SpecversionClient, error) {
	chanSize := 50000
	numWorkers := 30

	receiver := make(chan *rpc.JsonRpcResult, chanSize)
	wsClient, err := InitWSClient(endpoint, receiver)
	if err != nil {
		return nil, err
	}

	svc := SpecversionClient{
		wsclient: wsClient,
		receiver: receiver,
	}
	for i := 0; i < numWorkers; i++ {
		go svc.readSpecVersions()
	}
	return &svc, nil
}

func (svc *SpecversionClient) readSpecVersions() {
	for {
		rawSpecV := <-svc.receiver
		callerChan, ok := svc.caller.Load(rawSpecV.Id)
		if ok {
			callerChan.(chan *rpc.JsonRpcResult) <- rawSpecV
		}
	}
}

func (svc *SpecversionClient) GetBlockSpecVersion(blockHeight int, blockHash string) (int, error) {
	responseChan := make(chan *rpc.JsonRpcResult)
	svc.caller.Store(blockHeight, responseChan)
	defer svc.caller.Delete(blockHeight)

	svc.wsclient.SendMessage(rpc.ChainGetRuntimeVersion(blockHeight, blockHash))

	specV := <-responseChan
	return specV.ToRuntimeVersion().SpecVersion, nil
}
