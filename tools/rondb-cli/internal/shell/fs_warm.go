/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

package shell

// RDRS dictionary warm-up (RONDB-1121 E8 flake control, finding F20):
// right after the feature-store tables are created, the RDRS NDB API
// dictionary cache may still be refreshing; the first RonSQL requests
// then either fail with the transient "Schema cache for table not up to
// date" error or, through a null cached index entry, crash RDRS
// (NdbDictionaryImpl::getIndex).  Every runner therefore touches each
// table once, serially and with retries, before its parallel workers
// start, so the parallel phase never races the refresh.

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/bind"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
)

const (
	warmAttempts = 20
	warmBackoff  = 250 * time.Millisecond
)

// warmStatements returns one cheap statement per feature-group table: a
// COUNT over a primary-key bound that matches nothing (an index range on
// the first key column, not a table scan).
func warmStatements() []string {
	var out []string
	for _, fg := range data.Schema() {
		var pk string
		var pkType string
		for _, f := range fg.Features {
			if f.Primary {
				pk, pkType = f.Name, f.Type
				break
			}
		}
		if pk == "" {
			continue
		}
		lit := "-1"
		if strings.HasPrefix(strings.ToLower(pkType), "string") {
			lit = bind.Str("#warm#")
		}
		out = append(out, "SELECT COUNT(*) AS `n` FROM `"+fg.TableName()+"` WHERE `"+pk+"` = "+lit+";")
	}
	return out
}

// fsWarmRDRS runs the warm-up against RDRS.  It returns an error only when
// a table never answers; a transient dictionary error is retried.
func (s *Shell) fsWarmRDRS(o verifyOpts) error {
	if s.restClient == nil {
		return nil
	}
	_, rd, _, err := s.fsEngines(o)
	if err != nil {
		return err
	}
	defer rd.Close()
	for _, st := range warmStatements() {
		var last exec.Response
		ok := false
		for i := 0; i < warmAttempts && !ok; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
			last = rd.Query(ctx, st)
			cancel()
			switch last.Outcome {
			case exec.OK:
				ok = true
			case exec.Retryable, exec.Error, exec.CleanReject:
				// the dictionary may still be refreshing; an unknown table
				// (the data set is not loaded) is reported below
				if !isTransientDictionary(last.Message) && last.Outcome != exec.Retryable {
					return fmt.Errorf("RDRS warm-up: %s: %s", st, firstLine(last.Message))
				}
				time.Sleep(warmBackoff)
			default:
				return fmt.Errorf("RDRS warm-up: %s: %s %s", st, last.Outcome, firstLine(last.Message))
			}
		}
		if !ok {
			return fmt.Errorf("RDRS warm-up: %s never answered: %s", st, firstLine(last.Message))
		}
	}
	return nil
}

func isTransientDictionary(message string) bool {
	for _, p := range []string{"Schema cache for table not up to date", "Invalid schema object version", "Table definition has changed"} {
		if strings.Contains(message, p) {
			return true
		}
	}
	return false
}
