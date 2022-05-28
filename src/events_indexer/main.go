package main

import (
	"fmt"
	"go-dictionary/models"
	"strings"
	"sync"

	scalecodec "github.com/itering/scale.go"
	"github.com/itering/scale.go/types"
	"github.com/itering/scale.go/utiles"
)

const (
	eventQuery = `{"id":%d,"method":"state_getStorage","params":["0x26aa394eea5630e07c48ae0c9558cef780d41e5e16056765bc8461851072c9d7","%s"],"jsonrpc":"2.0"}`
)

func processEvents(wg sync.WaitGroup) {
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
