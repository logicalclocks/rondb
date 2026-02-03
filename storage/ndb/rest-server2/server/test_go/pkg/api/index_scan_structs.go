/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026 Hopsworks AB
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

package api

type IndexScanQuery struct {
	Limit       int           `json:"limit"`
	ReadColumns *[]ReadColumn `json:"readColumns"`
	Filters     *ScanFilter   `json:"filters"`
	Index       *IndexScan    `json:"index"`
}

/*
ScanFilter represents a recursive binary tree filter structure.

The filter consists of two types of nodes:
1. Logical operators (internal nodes):

  - op: "AND" | "OR" | "NAND" | "NOR"

  - args: array of sub-filters (each can be logical or comparison)
    Example:
    {
    "op": "AND",
    "args": [...]
    }

    2. Comparison operators (leaf nodes):
    a. CMP (comparison):

  - op: "CMP"

  - column: column name

  - cond: condition ("GT", "GE", "LT", "LE", "EQ", "NE")

  - value: comparison value
    Example:
    {
    "op": "CMP",
    "column": "val_1",
    "cond": "LE",
    "value": 30
    }

    b. ISNOTNULL (null check):

  - op: "ISNOTNULL"

  - column: column name
    Example:
    {
    "op": "ISNOTNULL",
    "column": "content"
    }

Complete example representing: (content IS NOT NULL AND pk > 2) AND (val_1 <= 30 OR val_2 > 500)

	{
	  "op": "AND",
	  "args": [
	    {
	      "op": "AND",
	      "args": [
	        {"op": "ISNOTNULL", "column": "content"},
	        {"op": "CMP", "column": "pk", "cond": "GT", "value": 2}
	      ]
	    },
	    {
	      "op": "OR",
	      "args": [
	        {"op": "CMP", "column": "val_1", "cond": "LE", "value": 30},
	        {"op": "CMP", "column": "val_2", "cond": "GT", "value": 500}
	      ]
	    }
	  ]
	}
*/
type ScanFilter struct {
	Op     string        `json:"op"`
	Args   []*ScanFilter `json:"args,omitempty"`
	Column string        `json:"column,omitempty"`
	Cond   string        `json:"cond,omitempty"`
	Value  any           `json:"value,omitempty"`
}

type IndexScan struct {
	Name       string      `json:"name"`
	KeyColumns []string    `json:"key_columns"`
	Ranges     []RangeScan `json:"ranges"`
	Order      string      `json:"order"`
}

type RangeScan struct {
	Lower BoundedScan `json:"lower"`
	Upper BoundedScan `json:"upper"`
}

type BoundedScan struct {
	Values    []any `json:"values"`
	Inclusive bool  `json:"inclusive"`
}

type IndexScanResponse struct {
	Data []map[string]any `json:"data"`
	Rows int              `json:"rows"`
}

type IndexTestInfo struct {
	IndexScanReq         IndexScanQuery
	Table                string
	DB                   string
	ExpectedHttpCode     int
	BodyContains         string
	RowsOrder            bool
	SkipMySQLValidation  bool // When true, skip MySQL comparison. Default false (validate).
}
