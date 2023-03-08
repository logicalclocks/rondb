package validators

import (
	"errors"
	"fmt"
)

func ValidateDBIdentifier(identifier *string) error {
	if identifier == nil {
		return errors.New("identifier is nil")
	} else if len(*identifier) < 1 {
		return errors.New("identifier is empty")
	} else if len(*identifier) > 64 {
		return fmt.Errorf("identifier is too large: %s", *identifier)
	}

	//https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
	for _, r := range *identifier {
		if !((r >= rune(0x0001) && r <= rune(0x007F)) || (r >= rune(0x0080) && r <= rune(0x0FFF))) {
			return fmt.Errorf("identifier carries an invalid character '%U' ", r)
		}
	}
	return nil
}
