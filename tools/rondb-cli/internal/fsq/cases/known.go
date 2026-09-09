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

package cases

// Known is the expectation table (random_generator.md §5.3, findings in
// mysql-test/suite/ronsql_fs/findings/smoke.md): the single place where
// "known engine outcome" lives.  A case referencing an entry reports
// REJECT(expected) / KNOWN-WRONG / KNOWN-ERROR instead of FAIL; a case that
// unexpectedly passes reports PASS(was-...) so the entry can be retired.
var Known = map[string]*Expect{
	// Hopsworks collect CTE form: non-aggregating CTE body over a partial key (risk R1).
	"S6-cte": {Finding: "F0", Pattern: "Non-aggregating CTE body is not a single-row key lookup"},
	// MySQL prints FLOAT with display precision; RonSQL prints the exact binary32 value.
	"F4": {Finding: "F4", Pattern: "FLOAT display precision"},
	// DECIMAL(18,2) beyond 2^53 cents loses precision on RonSQL's DOUBLE path.
	"F5": {Finding: "F5", Pattern: "DECIMAL precision loss"},
	// BIGINT SUM overflow is a clean NDB error (MySQL widens to DECIMAL).
	"F6": {Finding: "F6", Pattern: "arithmetic operation results overflow"},
	// VARBINARY cannot be projected by the pass-through printer.
	"F7": {Finding: "F7", Pattern: "Unsupported column type"},
	// MIN/MAX over a DATE/TIMESTAMP column is unquoted in JSON output (RDRS
	// default): the body is not JSON.  Verify-only (Case.KnownError).
	"F9": {Finding: "F9", Pattern: "malformed JSON result"},
}
