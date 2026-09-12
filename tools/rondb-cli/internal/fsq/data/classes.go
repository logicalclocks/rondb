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

package data

// KeyClass names a property of an entity that tests select by arithmetic
// instead of by computing results (data_model.md §6).
type KeyClass int

const (
	// ClassAny is every customer.
	ClassAny KeyClass = iota
	// ClassNoTx: no transactions (c mod 16 = 0).
	ClassNoTx
	// ClassOneTx: exactly one transaction.
	ClassOneTx
	// ClassFiveTx: exactly five transactions.
	ClassFiveTx
	// ClassFiftyTx: exactly fifty transactions.
	ClassFiftyTx
	// ClassMaxTx: three hundred transactions (crosses the 256-row batch boundary).
	ClassMaxTx
	// ClassDayBounds: daily rows with offset 0, so rows sit exactly on the
	// 1 / 7 / 30 / 90 day window bounds.
	ClassDayBounds
	// ClassHourly: hourly rows (a 1 h window sees at most one row).
	ClassHourly
	// ClassNullRegion: NULL region hop.
	ClassNullRegion
	// ClassDanglingRegion: region hop to a missing regions_1 row.
	ClassDanglingRegion
	// ClassNullCountry: region whose country hop is NULL.
	ClassNullCountry
	// ClassDanglingCountry: region whose country hop is missing.
	ClassDanglingCountry
	// ClassNoSessions: no sessions (c mod 8 = 0).
	ClassNoSessions
	// ClassUpperKey: string key with the upper-case prefix (c mod 10 = 0).
	ClassUpperKey
	// ClassMissing: customer id outside 1..E (no entity row at all).
	ClassMissing
)

var keyClassNames = map[KeyClass]string{
	ClassAny: "any", ClassNoTx: "no-tx", ClassOneTx: "one-tx", ClassFiveTx: "five-tx",
	ClassFiftyTx: "fifty-tx", ClassMaxTx: "max-tx", ClassDayBounds: "day-bounds",
	ClassHourly: "hourly", ClassNullRegion: "null-region", ClassDanglingRegion: "dangling-region",
	ClassNullCountry: "null-country", ClassDanglingCountry: "dangling-country",
	ClassNoSessions: "no-sessions", ClassUpperKey: "upper-key", ClassMissing: "missing",
}

// String returns the class name used in case ids.
func (k KeyClass) String() string { return keyClassNames[k] }

// InClass reports whether customer c (1..E) belongs to the class.
func InClass(k KeyClass, c int64, sc Scale) bool {
	switch k {
	case ClassAny:
		return c >= 1 && c <= sc.E
	case ClassNoTx:
		return c%16 == 0
	case ClassOneTx:
		return c%16 == 1
	case ClassFiveTx:
		return c%16 == 4 || c%16 == 5
	case ClassFiftyTx:
		return c%16 == 13
	case ClassMaxTx:
		return c%16 == 15
	case ClassDayBounds:
		return c%3 == 0 && (c/16)%4 == 2 && NTx(c) > 0
	case ClassHourly:
		return (c/16)%4 == 0 && NTx(c) > 0
	case ClassNullRegion:
		return c%13 == 0
	case ClassDanglingRegion:
		return c%29 == 0 && c%13 != 0
	case ClassNullCountry:
		r := RegionOf(c)
		return r.Valid && r.V <= RegionCount && r.V%17 == 0
	case ClassDanglingCountry:
		r := RegionOf(c)
		return r.Valid && r.V <= RegionCount && r.V%19 == 0 && r.V%17 != 0
	case ClassNoSessions:
		return c%8 == 0
	case ClassUpperKey:
		return c%10 == 0
	case ClassMissing:
		return c > sc.E
	}
	return false
}

// Pick returns the first n customer ids of the class, in increasing order.
// ClassMissing returns ids just above E.
func Pick(k KeyClass, n int, sc Scale) []int64 {
	var out []int64
	if k == ClassMissing {
		for c := sc.E + 1; len(out) < n; c++ {
			out = append(out, c)
		}
		return out
	}
	for c := int64(1); c <= sc.E && len(out) < n; c++ {
		if InClass(k, c, sc) {
			out = append(out, c)
		}
	}
	return out
}
