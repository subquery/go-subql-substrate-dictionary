package connection

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/itering/substrate-api-rpc/rpc"
)

type WsClient struct {
	sync.Mutex
	endpoint         string
	wsPool           []*websocket.Conn
	receivedMessages chan *[]byte
	sender           chan *rpc.JsonRpcResult //chan of the function calling the ws
}

func InitWSClient(endpoint string, sender chan *rpc.JsonRpcResult) (*WsClient, error) {
	numSockets := 5
	messagesChanSize := 50000
	receivedMessages := make(chan *[]byte, messagesChanSize)

	wsClient := &WsClient{
		endpoint:         endpoint,
		sender:           sender,
		receivedMessages: receivedMessages,
	}

	err := wsClient.connectPool(numSockets)
	if err != nil {
		return &WsClient{}, errors.New(fmt.Sprintf("Could not start websocket: %v", err))
	}

	return wsClient, nil
}

//connect the ws pool to the endpoint
func (c *WsClient) connectPool(numSockets int) error {
	for i := 0; i < numSockets; i++ {
		go func() {
			conn, _, err := websocket.DefaultDialer.Dial(c.endpoint, nil)
			if err != nil {
				fmt.Printf("Error connecting ws %d. Trying to reconnect...\n", err)
				return
			}
			c.Lock()
			c.wsPool = append(c.wsPool, conn)
			go c.workerGetMessage(len(c.wsPool) - 1)
			go c.readWSMessages(len(c.wsPool) - 1)
			c.Unlock()
		}()
	}
	return nil
}

func (c *WsClient) workerGetMessage(workerId int) {
	for {
		msg := <-c.receivedMessages
		fmt.Println(string(*msg))
		c.wsPool[workerId].WriteMessage(1, *msg)
	}
}

func (c *WsClient) readWSMessages(workerId int) {
	//add new socket in pool if the old one closes
	defer func() {
		conn, _, err := websocket.DefaultDialer.Dial(c.endpoint, nil)
		if err != nil {
			return
		}
		c.Lock()
		c.wsPool = append(c.wsPool, conn)
		go c.workerGetMessage(len(c.wsPool) - 1)
		go c.readWSMessages(len(c.wsPool) - 1)
		c.Unlock()
	}()
	for {
		v := &rpc.JsonRpcResult{}
		err := c.wsPool[workerId].ReadJSON(v)
		if err != nil {
			fmt.Printf("Ws disconnected %d: %v. Trying to reconnect...\n", workerId, err)
			c.wsPool[workerId].Close()
			return
		}
		c.sender <- v
	}
}

func (c *WsClient) SendMessage(message []byte) {
	c.receivedMessages <- &message
}
