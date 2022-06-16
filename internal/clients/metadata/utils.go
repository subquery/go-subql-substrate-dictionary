package metadata

import "time"

func getTimestamp() int64 {
	return time.Now().Unix()
}

func getTimeString() string {
	return time.Now().Format(time.RFC3339)
}
