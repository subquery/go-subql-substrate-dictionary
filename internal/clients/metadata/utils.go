package metadata

import "time"

func getTimestamp() (int, string) {
	current := time.Now()
	return int(current.Unix()), current.Format(time.RFC3339)
}
