package connection

import (
	"sync"

	"github.com/itering/substrate-api-rpc/rpc"
)

type BlockHashClient struct {
	wsclient *WsClient
	receiver chan *rpc.JsonRpcResult
	caller   sync.Map
}

func NewBlockHashClient(endpoint string) (*BlockHashClient, error) {
	chanSize := 50000
	numWorkers := 50

	receiver := make(chan *rpc.JsonRpcResult, chanSize)
	wsClient, err := InitWSClient(endpoint, receiver)
	if err != nil {
		return nil, err
	}

	c := BlockHashClient{
		wsclient: wsClient,
		receiver: receiver,
	}
	for i := 0; i < numWorkers; i++ {
		go c.readBlockHash()
	}
	return &c, nil
}

func (c *BlockHashClient) readBlockHash() {
	for {
		rawHash := <-c.receiver
		callerChan, ok := c.caller.Load(rawHash.Id)
		if ok {
			callerChan.(chan *rpc.JsonRpcResult) <- rawHash
		}
	}
}

func (c *BlockHashClient) GetBlockHash(blockHeight int) (string, error) {
	responseChan := make(chan *rpc.JsonRpcResult)
	c.caller.Store(blockHeight, responseChan)
	defer c.caller.Delete(blockHeight)

	c.wsclient.SendMessage(rpc.ChainGetBlockHash(blockHeight, blockHeight))

	hash := <-responseChan
	return hash.ToString()
}
