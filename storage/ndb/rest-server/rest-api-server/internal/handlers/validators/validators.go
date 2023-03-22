/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
