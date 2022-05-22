package main

import (
	"flag"
	"go-dictionary/internal/clients"
	"go-dictionary/internal/config"
	"go-dictionary/internal/messages"
)

func main() {
	var (
		configFilePath          string
		dictionaryConfiguration config.Config
		dictionaryMessage       *messages.DictionaryMessage
	)

	flag.StringVar(&configFilePath, "configfile", "", "path to config file")
	flag.Parse()
	if configFilePath == "" {
		messages.NewDictionaryMessage(messages.LOG_LEVEL_INFO, "", nil, messages.CONFIG_NO_CUSTOM_PATH_SPECIFIED).ConsoleLog()
		dictionaryConfiguration, dictionaryMessage = config.LoadConfig(nil)
	} else {
		dictionaryConfiguration, dictionaryMessage = config.LoadConfig(&configFilePath)
	}

	if dictionaryMessage != nil {
		dictionaryMessage.ConsoleLog()
		return
	}

	orchestrator := clients.NewOrchestrator(dictionaryConfiguration)
	defer orchestrator.Close()

	orchestrator.Run()
}
