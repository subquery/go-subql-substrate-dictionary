package config

import (
	"encoding/json"
	"go-dictionary/internal/messages"
	"os"
)

const (
	defaultConfigFilePath = "config.json"
)

// LoadConfig tries to load the service config from a config file given as a parameter. If the filename is a nil
// string pointer, it defaults to a constant file path "config.json"
func LoadConfig(configFilePath *string) Config {
	var (
		configPath       string
		dictionaryConfig Config
	)

	configPath = defaultConfigFilePath
	if configFilePath != nil {
		configPath = *configFilePath
	}
	messages.NewDictionaryMessage(messages.LOG_LEVEL_INFO, "", nil, messages.CONFIG_STARTED_LOADING, configPath).ConsoleLog()

	configFile, err := os.Open(configPath)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(LoadConfig),
			err,
			messages.CONFIG_FAILED_TO_OPEN_FILE,
			configPath,
		).ConsoleLog()
	}

	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&dictionaryConfig)
	if err != nil {
		messages.NewDictionaryMessage(
			messages.LOG_LEVEL_ERROR,
			messages.GetComponent(LoadConfig),
			err,
			messages.CONFIG_WRONG_FILE_FORMAT,
		).ConsoleLog()
	}

	messages.NewDictionaryMessage(messages.LOG_LEVEL_SUCCESS, "", nil, messages.CONFIG_FINISHED_LOADING).ConsoleLog()
	return dictionaryConfig
}
