package da

import "time"
import "encoding/json"

// Document represents the document of a version.
type Document struct {
	ID       string
	Version  int64
	Modified time.Time
	Seq      int64
	Deleted  bool

	Data json.RawMessage
}
