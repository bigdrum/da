package da

import "time"
import "encoding/json"

// Document represents the document of a version.
type Document struct {
	ID       string    `json:"id,omitempty"`
	Version  int64     `json:"version,omitempty"`
	Modified time.Time `json:"modified,omitempty"`
	Seq      int64     `json:"seq,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`

	Data json.RawMessage `json:"data,omitempty"`
}
