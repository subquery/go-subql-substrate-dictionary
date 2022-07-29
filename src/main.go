package main

import (
	"flag"
	"fmt"
	"go-dictionary/internal/clients"
	"go-dictionary/internal/config"
	"go-dictionary/internal/messages"
)

func main() {
	var (
		configFilePath          string
		dictionaryConfiguration config.Config
		batchBlock              int
	)

	flag.StringVar(&configFilePath, "config", "", "path to config file")
	flag.StringVar(&configFilePath, "c", "", "path to config file")
	flag.IntVar(&batchBlock, "b", 0, "specific a block to batch")
	flag.Parse()
	if configFilePath == "" {
		messages.NewDictionaryMessage(messages.LOG_LEVEL_INFO, "", nil, messages.CONFIG_NO_CUSTOM_PATH_SPECIFIED).ConsoleLog()
		dictionaryConfiguration = config.LoadConfig(nil)
	} else {
		dictionaryConfiguration = config.LoadConfig(&configFilePath)
	}

	orchestrator := clients.NewOrchestrator(dictionaryConfiguration)
	defer orchestrator.Close()

	if batchBlock != 0 {
		fmt.Printf("specific block %d.", batchBlock)
		orchestrator.Batch(batchBlock)
	} else {
		orchestrator.Run()
	}
}
