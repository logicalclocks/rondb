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

import (
	"fmt"
	"strings"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

// GoldenDataVersion identifies the hand-authored A6 data, not Java output.
// Report it alongside the captured fixture provenance and FSNow.
const GoldenDataVersion = "a6-v1"

// GoldenEntityKey maps event entity ids to the numeric or string-key data.
// Events exist for 1..3; 4 is a missing-entity probe.
func GoldenEntityKey(entity int64, stringKey bool) string {
	if !stringKey {
		return itoa(entity)
	}
	switch entity {
	case 2:
		return "O'Brien"
	case 3:
		return "café"
	default:
		return fmt.Sprintf("entity-%d", entity)
	}
}

// GoldenRows returns SQL literals in the captured feature-column order.
// It does not create, load or drop anything. Each fixture needs isolated
// setup: the corpus reuses events_1 for incompatible schemas. These rows
// must not be added to the E4 Tables() catalog or loaded over existing data.
// Unknown layouts fail rather than silently receiving generic values.
// Array tags are NULL; binary payloads are opaque bytes, not array encoding.
func GoldenRows(fg spec.FeatureGroup) ([][]string, error) {
	var columns []string
	for _, f := range fg.Features {
		if f.OfflineOnly || f.OnlineType != "" || f.DefaultValue != nil {
			return nil, fmt.Errorf("A6 data: unsupported column override on %s.%s", fg.TableName(), f.Name)
		}
		column := f.Name + ":" + f.Type
		if f.Primary {
			column += "*"
		}
		columns = append(columns, column)
	}
	layout := fg.Name + "|" + fg.EventTime + "|" + strings.Join(columns, ",")
	const eventTail = ",amount:bigint,fee:int,category:string,tags:array<string>"
	switch layout {
	case "events|event_time|entity_id:bigint*,event_time:timestamp*" + eventTail:
		return goldenEventRows(false, false, false), nil
	case "events|event_time|entity_id:string*,event_time:timestamp*" + eventTail:
		return goldenEventRows(true, false, false), nil
	case "events|event_time|entity_id:bigint*,currency:string*,event_time:timestamp*" + eventTail:
		return goldenEventRows(false, true, false), nil
	case "events||entity_id:bigint*,sequence_no:bigint*" + eventTail:
		return goldenEventRows(false, false, true), nil
	case "profiles||entity_id:bigint*,region_id:int,region_code:string,tier:string":
		// 1/2 hit distinct composite keys; 3/7 have NULL key components;
		// 4/8 miss a region; 5/6 miss a country; 9 hits NULL projections.
		// Entity 10 is deliberately absent.
		return [][]string{
			{"1", "10", "'A'", "'gold'"},
			{"2", "10", "'B'", "'silver'"},
			{"3", "NULL", "'A'", "'null-region-id'"},
			{"4", "999", "'A'", "'missing-region'"},
			{"5", "20", "'A'", "'null-country'"},
			{"6", "30", "'A'", "'missing-country'"},
			{"7", "10", "NULL", "'null-region-code'"},
			{"8", "10", "'C'", "'wrong-region-code'"},
			{"9", "40", "'A'", "NULL"},
		}, nil
	case "regions||region_id:int*,region_code:string*,country_id:int,region_name:string,payload:binary":
		return [][]string{
			{"10", "'A'", "100", "'north'", "X'0001'"},
			{"10", "'B'", "200", "'south'", "X'ff00'"},
			{"20", "'A'", "NULL", "'null-country'", "NULL"},
			{"30", "'A'", "999", "'missing-country'", "X''"},
			{"40", "'A'", "300", "NULL", "X'1020'"},
		}, nil
	case "countries||country_id:int*,country_name:string":
		return [][]string{{"100", "'Sweden'"}, {"200", "'France'"}, {"300", "NULL"}}, nil
	default:
		return nil, fmt.Errorf("A6 data: unsupported layout for %s: %s", fg.TableName(), layout)
	}
}

func goldenEventRows(stringKey, composite, sequence bool) [][]string {
	seeds := []struct {
		entity, age int64 // seconds before FSNow
		amount, fee OptInt
		category    string
	}{
		{1, 7200, some(-5), some(2), "old"},
		{1, 3601, some(0), some(1), "before-cutoff"},
		{1, 3600, some(10), some(3), "at-cutoff"},
		{1, 3599, some(12), some(4), "O'Brien-first"},
		{1, 1800, some(13), some(5), "O'Brien-second"},
		{1, 60, some(20), null, "recent"},
		{1, 1, null, some(7), "null-amount"},
		{1, 0, some(30), some(8), "now"},
		{2, 30, some(5), some(2), "one-row"},
		{3, 120, null, null, "all-null"},
		{3, 60, null, some(9), "all-null"},
	}
	currencies := []string{"EUR"}
	if composite {
		currencies = append(currencies, "USD")
	}
	var rows [][]string
	for currencyIndex, currency := range currencies {
		for i, seed := range seeds {
			key := GoldenEntityKey(seed.entity, stringKey)
			if stringKey {
				key = SQLString(key)
			}
			row := []string{key}
			if composite {
				row = append(row, SQLString(currency))
			}
			if sequence {
				row = append(row, itoa(int64(i+1)))
			} else {
				row = append(row, SQLString(FormatTimestamp(FSNow.Add(-time.Duration(seed.age)*time.Second))))
			}
			amount := seed.amount
			if amount.Valid {
				amount.V += int64(currencyIndex * 100) // Distinguish composite-key partners.
			}
			row = append(row, SQLOptInt(amount), SQLOptInt(seed.fee), SQLString(seed.category), "NULL")
			rows = append(rows, row)
		}
	}
	return rows
}
