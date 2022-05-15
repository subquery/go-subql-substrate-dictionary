package models

type Event struct {
	Id          string //blockheight-eventId
	Module      string
	Event       string
	BlockHeight int
}
