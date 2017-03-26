package da

import "time"

// Document represents a document of a version.
type Document struct {
	ID       string
	Version  int64
	Data     interface{}
	Modified time.Time
}
