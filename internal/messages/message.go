package messages

import "fmt"

func NewDictionaryMessage(level DictionaryLogLevel, component string, err error, formatString string, additionalInfo ...interface{}) *DictionaryMessage {
	return &DictionaryMessage{
		LogLevel:       level,
		Component:      component,
		Error:          err,
		FormatString:   formatString,
		AdditionalInfo: additionalInfo,
	}
}

func (dictMsg *DictionaryMessage) ConsoleLog() {
	var msg string
	switch dictMsg.LogLevel {
	case LOG_LEVEL_INFO, LOG_LEVEL_SUCCESS, LOG_LEVEL_WARNING:
		msg = dictMsg.formatMessage()
	case LOG_LEVEL_ERROR:
		msg = dictMsg.formatError()
	}
	fmt.Println(msg)
}

func (dictMsg *DictionaryMessage) formatMessage() string {
	// [LOG_LEVEL] custom_message
	fmtString := "[%s] " + dictMsg.FormatString
	additionalArgs := append([]interface{}{dictMsg.LogLevel}, dictMsg.AdditionalInfo...)
	msg := fmt.Sprintf(fmtString, additionalArgs...)
	switch dictMsg.LogLevel {
	case LOG_LEVEL_INFO:
		msg = blue + msg + reset
	case LOG_LEVEL_SUCCESS:
		msg = green + msg + reset
	case LOG_LEVEL_WARNING:
		msg = yellow + msg + reset
	default:
		msg = white + msg + reset
	}
	return msg
}

func (dictMsg *DictionaryMessage) formatError() string {
	// [LOG_LEVEL][COMPONENT] custom_message: error_message
	fmtString := "[%s][%s] " + dictMsg.FormatString + ": [%v]"
	var additionalArgs []interface{}
	additionalArgs = append(additionalArgs, dictMsg.LogLevel, dictMsg.Component)
	additionalArgs = append(additionalArgs, dictMsg.AdditionalInfo...)
	additionalArgs = append(additionalArgs, dictMsg.Error)
	msg := fmt.Sprintf(fmtString, additionalArgs...)
	msg = red + msg + reset
	return msg
}
