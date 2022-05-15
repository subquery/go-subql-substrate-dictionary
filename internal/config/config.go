package config

import (
	"encoding/json"
	"go-dictionary/internal/messages"
	"os"
	"reflect"
)

const (
	defaultConfigFilePath = "config.json"
)

// LoadConfig tries to load the service config from a config file given as a parameter. If the filename is a nil
// string pointer, it defaults to a constant file path "config.json"
func LoadConfig(configFilePath *string) (Config, *messages.DictionaryMessage) {
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
		return dictionaryConfig, messages.NewDictionaryMessage(messages.LOG_LEVEL_ERROR, reflect.TypeOf("").PkgPath(), err, "")
	}

	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&dictionaryConfig)
	if err != nil {
		return dictionaryConfig, messages.NewDictionaryMessage(messages.LOG_LEVEL_ERROR, reflect.TypeOf("").PkgPath(), err, "")
	}

	messages.NewDictionaryMessage(messages.LOG_LEVEL_SUCCESS, "", nil, messages.CONFIG_FINISHED_LOADING).ConsoleLog()
	return dictionaryConfig, nil
}
