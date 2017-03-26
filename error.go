package da

import (
	"fmt"
)

// Error implements error interface and Da uses it to return errors where
// the client would be interested about its kind.
type Error struct {
	Kind    ErrorKind
	Message string
	Err     error // Underlying error.
}

// ErrorKind defines the kind of error.
type ErrorKind uint8

// Values of ErrorKind.
const (
	ErrOther    ErrorKind = iota // Unclassified error.
	ErrConflict                  // There is a version conflict.
	ErrNotFound                  // Requested resource not found.
)

func (e Error) Error() string {
	// TODO: Return kind.
	return e.Message
}

// errorf creates an error.
func errorf(kind ErrorKind, err error, fmtMsg string, args ...interface{}) Error {
	return Error{
		Kind:    kind,
		Message: fmt.Sprintf(fmtMsg, args...),
		Err:     err,
	}
}
