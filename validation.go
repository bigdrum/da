package da

import (
	"fmt"
	"regexp"
)

var namePattern = regexp.MustCompile(`^[a-zA-Z_0-9]+$`)

func checkName(s string) error {
	if namePattern.MatchString(s) {
		return nil
	}
	return fmt.Errorf("unsupported name: %s", s)
}
