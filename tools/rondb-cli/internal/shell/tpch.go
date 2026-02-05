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

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/client"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/dsl"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/ui"
)

// TPC-H scale factor row counts
const (
	tpchRegionCount   = 5     // Fixed
	tpchNationCount   = 25    // Fixed
	tpchCustomerBase  = 150000
	tpchSupplierBase  = 10000
	tpchPartBase      = 200000
	tpchPartsuppBase  = 800000  // 4 per part
	tpchOrdersBase    = 1500000
	tpchLineitemBase  = 6000000 // ~4 per order average
)

// TPC-H reference data
var tpchRegions = []string{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}

var tpchNations = []struct {
	name      string
	regionKey int
}{
	{"ALGERIA", 0}, {"ARGENTINA", 1}, {"BRAZIL", 1}, {"CANADA", 1}, {"EGYPT", 4},
	{"ETHIOPIA", 0}, {"FRANCE", 3}, {"GERMANY", 3}, {"INDIA", 2}, {"INDONESIA", 2},
	{"IRAN", 4}, {"IRAQ", 4}, {"JAPAN", 2}, {"JORDAN", 4}, {"KENYA", 0},
	{"MOROCCO", 0}, {"MOZAMBIQUE", 0}, {"PERU", 1}, {"CHINA", 2}, {"ROMANIA", 3},
	{"SAUDI ARABIA", 4}, {"VIETNAM", 2}, {"RUSSIA", 3}, {"UNITED KINGDOM", 3}, {"UNITED STATES", 1},
}

var tpchSegments = []string{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"}
var tpchPriorities = []string{"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"}
var tpchShipModes = []string{"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"}
var tpchShipInstructs = []string{"DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"}
var tpchContainers = []string{"SM CASE", "SM BOX", "SM BAG", "SM JAR", "SM PACK", "SM PKG", "SM CAN", "SM DRUM",
	"MED CASE", "MED BOX", "MED BAG", "MED JAR", "MED PACK", "MED PKG", "MED CAN", "MED DRUM",
	"LG CASE", "LG BOX", "LG BAG", "LG JAR", "LG PACK", "LG PKG", "LG CAN", "LG DRUM",
	"JUMBO CASE", "JUMBO BOX", "JUMBO BAG", "JUMBO JAR", "JUMBO PACK", "JUMBO PKG", "JUMBO CAN", "JUMBO DRUM",
	"WRAP CASE", "WRAP BOX", "WRAP BAG", "WRAP JAR", "WRAP PACK", "WRAP PKG", "WRAP CAN", "WRAP DRUM"}
var tpchTypes = []string{"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"}
var tpchTypeMaterials = []string{"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"}
var tpchBrands = []string{"Brand#11", "Brand#12", "Brand#13", "Brand#14", "Brand#15",
	"Brand#21", "Brand#22", "Brand#23", "Brand#24", "Brand#25",
	"Brand#31", "Brand#32", "Brand#33", "Brand#34", "Brand#35",
	"Brand#41", "Brand#42", "Brand#43", "Brand#44", "Brand#45",
	"Brand#51", "Brand#52", "Brand#53", "Brand#54", "Brand#55"}

// TPCHTableInfo contains information about a TPC-H table to load
type TPCHTableInfo struct {
	Name      string
	RowCount  func(sf int) int
	Generator func(sf int, startRow, endRow int, rng *rand.Rand) []dsl.BatchWriteOperation
}

// GetTPCHTables returns the list of TPC-H tables in dependency order
func GetTPCHTables() []TPCHTableInfo {
	return []TPCHTableInfo{
		{Name: "region", RowCount: func(sf int) int { return tpchRegionCount }, Generator: generateRegionRows},
		{Name: "nation", RowCount: func(sf int) int { return tpchNationCount }, Generator: generateNationRows},
		{Name: "supplier", RowCount: func(sf int) int { return tpchSupplierBase * sf }, Generator: generateSupplierRows},
		{Name: "customer", RowCount: func(sf int) int { return tpchCustomerBase * sf }, Generator: generateCustomerRows},
		{Name: "part", RowCount: func(sf int) int { return tpchPartBase * sf }, Generator: generatePartRows},
		{Name: "partsupp", RowCount: func(sf int) int { return tpchPartsuppBase * sf }, Generator: generatePartsuppRows},
		{Name: "orders", RowCount: func(sf int) int { return tpchOrdersBase * sf }, Generator: generateOrdersRows},
		{Name: "lineitem", RowCount: func(sf int) int { return tpchLineitemBase * sf }, Generator: generateLineitemRows},
	}
}

// TPC-H table creation SQL statements
var tpchTableDDL = map[string]string{
	"region": `CREATE TABLE IF NOT EXISTS tpch.region (
		r_regionkey INT NOT NULL,
		r_name CHAR(25) NOT NULL,
		r_comment VARCHAR(152),
		PRIMARY KEY (r_regionkey)
	) ENGINE=NDB`,

	"nation": `CREATE TABLE IF NOT EXISTS tpch.nation (
		n_nationkey INT NOT NULL,
		n_name CHAR(25) NOT NULL,
		n_regionkey INT NOT NULL,
		n_comment VARCHAR(152),
		PRIMARY KEY (n_nationkey)
	) ENGINE=NDB`,

	"supplier": `CREATE TABLE IF NOT EXISTS tpch.supplier (
		s_suppkey INT NOT NULL,
		s_name CHAR(25) NOT NULL,
		s_address VARCHAR(40) NOT NULL,
		s_nationkey INT NOT NULL,
		s_phone CHAR(15) NOT NULL,
		s_acctbal DECIMAL(15,2) NOT NULL,
		s_comment VARCHAR(101),
		PRIMARY KEY (s_suppkey)
	) ENGINE=NDB`,

	"customer": `CREATE TABLE IF NOT EXISTS tpch.customer (
		c_custkey INT NOT NULL,
		c_name VARCHAR(25) NOT NULL,
		c_address VARCHAR(40) NOT NULL,
		c_nationkey INT NOT NULL,
		c_phone CHAR(15) NOT NULL,
		c_acctbal DECIMAL(15,2) NOT NULL,
		c_mktsegment CHAR(10) NOT NULL,
		c_comment VARCHAR(117),
		PRIMARY KEY (c_custkey)
	) ENGINE=NDB`,

	"part": `CREATE TABLE IF NOT EXISTS tpch.part (
		p_partkey INT NOT NULL,
		p_name VARCHAR(55) NOT NULL,
		p_mfgr CHAR(25) NOT NULL,
		p_brand CHAR(10) NOT NULL,
		p_type VARCHAR(25) NOT NULL,
		p_size INT NOT NULL,
		p_container CHAR(10) NOT NULL,
		p_retailprice DECIMAL(15,2) NOT NULL,
		p_comment VARCHAR(23),
		PRIMARY KEY (p_partkey)
	) ENGINE=NDB`,

	"partsupp": `CREATE TABLE IF NOT EXISTS tpch.partsupp (
		ps_partkey INT NOT NULL,
		ps_suppkey INT NOT NULL,
		ps_availqty INT NOT NULL,
		ps_supplycost DECIMAL(15,2) NOT NULL,
		ps_comment VARCHAR(199),
		PRIMARY KEY (ps_partkey, ps_suppkey)
	) ENGINE=NDB`,

	"orders": `CREATE TABLE IF NOT EXISTS tpch.orders (
		o_orderkey BIGINT NOT NULL,
		o_custkey INT NOT NULL,
		o_orderstatus CHAR(1) NOT NULL,
		o_totalprice DECIMAL(15,2) NOT NULL,
		o_orderdate DATE NOT NULL,
		o_orderpriority CHAR(15) NOT NULL,
		o_clerk CHAR(15) NOT NULL,
		o_shippriority INT NOT NULL,
		o_comment VARCHAR(79),
		PRIMARY KEY (o_orderkey)
	) ENGINE=NDB`,

	"lineitem": `CREATE TABLE IF NOT EXISTS tpch.lineitem (
		l_orderkey BIGINT NOT NULL,
		l_partkey INT NOT NULL,
		l_suppkey INT NOT NULL,
		l_linenumber INT NOT NULL,
		l_quantity DECIMAL(15,2) NOT NULL,
		l_extendedprice DECIMAL(15,2) NOT NULL,
		l_discount DECIMAL(15,2) NOT NULL,
		l_tax DECIMAL(15,2) NOT NULL,
		l_returnflag CHAR(1) NOT NULL,
		l_linestatus CHAR(1) NOT NULL,
		l_shipdate DATE NOT NULL,
		l_commitdate DATE NOT NULL,
		l_receiptdate DATE NOT NULL,
		l_shipinstruct CHAR(25) NOT NULL,
		l_shipmode CHAR(10) NOT NULL,
		l_comment VARCHAR(44),
		PRIMARY KEY (l_orderkey, l_linenumber)
	) ENGINE=NDB`,
}

// Row generators for each TPC-H table

func generateRegionRows(sf int, startRow, endRow int, rng *rand.Rand) []dsl.BatchWriteOperation {
	var ops []dsl.BatchWriteOperation
	for i := startRow; i < endRow && i < tpchRegionCount; i++ {
		op := dsl.BatchWriteOperation{
			Method:      "POST",
			RelativeURL: "tpch/region/pk-write",
			Body: dsl.PkWriteRequest{
				Filters: []dsl.Filter{
					{Column: "r_regionkey", Value: i},
				},
				WriteColumns: []dsl.WriteColumn{
					{Column: "r_name", Value: tpchRegions[i]},
					{Column: "r_comment", Value: generateComment(rng, 31, 115)},
				},
				OperationID: fmt.Sprintf("region_%d", i),
			},
		}
		ops = append(ops, op)
	}
	return ops
}

func generateNationRows(sf int, startRow, endRow int, rng *rand.Rand) []dsl.BatchWriteOperation {
	var ops []dsl.BatchWriteOperation
	for i := startRow; i < endRow && i < tpchNationCount; i++ {
		op := dsl.BatchWriteOperation{
			Method:      "POST",
			RelativeURL: "tpch/nation/pk-write",
			Body: dsl.PkWriteRequest{
				Filters: []dsl.Filter{
					{Column: "n_nationkey", Value: i},
				},
				WriteColumns: []dsl.WriteColumn{
					{Column: "n_name", Value: tpchNations[i].name},
					{Column: "n_regionkey", Value: tpchNations[i].regionKey},
					{Column: "n_comment", Value: generateComment(rng, 31, 114)},
				},
				OperationID: fmt.Sprintf("nation_%d", i),
			},
		}
		ops = append(ops, op)
	}
	return ops
}

func generateSupplierRows(sf int, startRow, endRow int, rng *rand.Rand) []dsl.BatchWriteOperation {
	var ops []dsl.BatchWriteOperation
	for i := startRow; i < endRow; i++ {
		suppKey := i + 1
		op := dsl.BatchWriteOperation{
			Method:      "POST",
			RelativeURL: "tpch/supplier/pk-write",
			Body: dsl.PkWriteRequest{
				Filters: []dsl.Filter{
					{Column: "s_suppkey", Value: suppKey},
				},
				WriteColumns: []dsl.WriteColumn{
					{Column: "s_name", Value: fmt.Sprintf("Supplier#%09d", suppKey)},
					{Column: "s_address", Value: generateAddress(rng)},
					{Column: "s_nationkey", Value: rng.Intn(tpchNationCount)},
					{Column: "s_phone", Value: generatePhone(rng)},
					{Column: "s_acctbal", Value: fmt.Sprintf("%.2f", -999.99+rng.Float64()*10998.99)},
					{Column: "s_comment", Value: generateComment(rng, 25, 100)},
				},
				OperationID: fmt.Sprintf("supplier_%d", suppKey),
			},
		}
		ops = append(ops, op)
	}
	return ops
}

func generateCustomerRows(sf int, startRow, endRow int, rng *rand.Rand) []dsl.BatchWriteOperation {
	var ops []dsl.BatchWriteOperation
	for i := startRow; i < endRow; i++ {
		custKey := i + 1
		op := dsl.BatchWriteOperation{
			Method:      "POST",
			RelativeURL: "tpch/customer/pk-write",
			Body: dsl.PkWriteRequest{
				Filters: []dsl.Filter{
					{Column: "c_custkey", Value: custKey},
				},
				WriteColumns: []dsl.WriteColumn{
					{Column: "c_name", Value: fmt.Sprintf("Customer#%09d", custKey)},
					{Column: "c_address", Value: generateAddress(rng)},
					{Column: "c_nationkey", Value: rng.Intn(tpchNationCount)},
					{Column: "c_phone", Value: generatePhone(rng)},
					{Column: "c_acctbal", Value: fmt.Sprintf("%.2f", -999.99+rng.Float64()*10998.99)},
					{Column: "c_mktsegment", Value: tpchSegments[rng.Intn(len(tpchSegments))]},
					{Column: "c_comment", Value: generateComment(rng, 29, 116)},
				},
				OperationID: fmt.Sprintf("customer_%d", custKey),
			},
		}
		ops = append(ops, op)
	}
	return ops
}

func generatePartRows(sf int, startRow, endRow int, rng *rand.Rand) []dsl.BatchWriteOperation {
	var ops []dsl.BatchWriteOperation
	for i := startRow; i < endRow; i++ {
		partKey := i + 1
		op := dsl.BatchWriteOperation{
			Method:      "POST",
			RelativeURL: "tpch/part/pk-write",
			Body: dsl.PkWriteRequest{
				Filters: []dsl.Filter{
					{Column: "p_partkey", Value: partKey},
				},
				WriteColumns: []dsl.WriteColumn{
					{Column: "p_name", Value: generatePartName(rng)},
					{Column: "p_mfgr", Value: fmt.Sprintf("Manufacturer#%d", rng.Intn(5)+1)},
					{Column: "p_brand", Value: tpchBrands[rng.Intn(len(tpchBrands))]},
					{Column: "p_type", Value: fmt.Sprintf("%s %s", tpchTypeMaterials[rng.Intn(len(tpchTypeMaterials))], tpchTypes[rng.Intn(len(tpchTypes))])},
					{Column: "p_size", Value: rng.Intn(50) + 1},
					{Column: "p_container", Value: tpchContainers[rng.Intn(len(tpchContainers))]},
					{Column: "p_retailprice", Value: fmt.Sprintf("%.2f", 900.0+float64(partKey%20001)/100.0)},
					{Column: "p_comment", Value: generateComment(rng, 5, 22)},
				},
				OperationID: fmt.Sprintf("part_%d", partKey),
			},
		}
		ops = append(ops, op)
	}
	return ops
}

func generatePartsuppRows(sf int, startRow, endRow int, rng *rand.Rand) []dsl.BatchWriteOperation {
	var ops []dsl.BatchWriteOperation
	supplierCount := tpchSupplierBase * sf
	if supplierCount == 0 {
		supplierCount = 1
	}

	for i := startRow; i < endRow; i++ {
		partKey := (i / 4) + 1
		suppIdx := i % 4
		suppKey := ((partKey + suppIdx*((supplierCount/4)+((partKey-1)/supplierCount))) % supplierCount) + 1

		op := dsl.BatchWriteOperation{
			Method:      "POST",
			RelativeURL: "tpch/partsupp/pk-write",
			Body: dsl.PkWriteRequest{
				Filters: []dsl.Filter{
					{Column: "ps_partkey", Value: partKey},
					{Column: "ps_suppkey", Value: suppKey},
				},
				WriteColumns: []dsl.WriteColumn{
					{Column: "ps_availqty", Value: rng.Intn(9999) + 1},
					{Column: "ps_supplycost", Value: fmt.Sprintf("%.2f", 1.0+rng.Float64()*999.0)},
					{Column: "ps_comment", Value: generateComment(rng, 49, 198)},
				},
				OperationID: fmt.Sprintf("partsupp_%d_%d", partKey, suppKey),
			},
		}
		ops = append(ops, op)
	}
	return ops
}

func generateOrdersRows(sf int, startRow, endRow int, rng *rand.Rand) []dsl.BatchWriteOperation {
	var ops []dsl.BatchWriteOperation
	customerCount := tpchCustomerBase * sf
	if customerCount == 0 {
		customerCount = 1
	}

	for i := startRow; i < endRow; i++ {
		orderKey := int64(i+1) * 4 // Orders use sparse keys (multiples of 4, no 1 or 3 mod 8)
		custKey := (rng.Intn(customerCount) + 1)

		// Order date between 1992-01-01 and 1998-08-02
		startDate := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
		dayOffset := rng.Intn(2406) // ~6.5 years of days
		orderDate := startDate.AddDate(0, 0, dayOffset)

		op := dsl.BatchWriteOperation{
			Method:      "POST",
			RelativeURL: "tpch/orders/pk-write",
			Body: dsl.PkWriteRequest{
				Filters: []dsl.Filter{
					{Column: "o_orderkey", Value: orderKey},
				},
				WriteColumns: []dsl.WriteColumn{
					{Column: "o_custkey", Value: custKey},
					{Column: "o_orderstatus", Value: []string{"F", "O", "P"}[rng.Intn(3)]},
					{Column: "o_totalprice", Value: fmt.Sprintf("%.2f", rng.Float64()*500000)},
					{Column: "o_orderdate", Value: orderDate.Format("2006-01-02")},
					{Column: "o_orderpriority", Value: tpchPriorities[rng.Intn(len(tpchPriorities))]},
					{Column: "o_clerk", Value: fmt.Sprintf("Clerk#%09d", rng.Intn(1000)+1)},
					{Column: "o_shippriority", Value: 0},
					{Column: "o_comment", Value: generateComment(rng, 19, 78)},
				},
				OperationID: fmt.Sprintf("orders_%d", orderKey),
			},
		}
		ops = append(ops, op)
	}
	return ops
}

func generateLineitemRows(sf int, startRow, endRow int, rng *rand.Rand) []dsl.BatchWriteOperation {
	var ops []dsl.BatchWriteOperation
	orderCount := tpchOrdersBase * sf
	partCount := tpchPartBase * sf
	supplierCount := tpchSupplierBase * sf
	if orderCount == 0 {
		orderCount = 1
	}
	if partCount == 0 {
		partCount = 1
	}
	if supplierCount == 0 {
		supplierCount = 1
	}

	// Approximately 4 line items per order
	for i := startRow; i < endRow; i++ {
		orderIdx := i / 4
		lineNumber := (i % 4) + 1
		orderKey := int64(orderIdx+1) * 4

		partKey := rng.Intn(partCount) + 1
		suppKey := rng.Intn(supplierCount) + 1

		quantity := float64(rng.Intn(50) + 1)
		extendedPrice := quantity * (900.0 + float64(partKey%20001)/100.0)
		discount := float64(rng.Intn(11)) / 100.0
		tax := float64(rng.Intn(9)) / 100.0

		// Ship date is order date + random days
		startDate := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
		orderDayOffset := rng.Intn(2406)
		orderDate := startDate.AddDate(0, 0, orderDayOffset)
		shipDate := orderDate.AddDate(0, 0, rng.Intn(121)+1)
		commitDate := orderDate.AddDate(0, 0, rng.Intn(90)+30)
		receiptDate := shipDate.AddDate(0, 0, rng.Intn(30)+1)

		// Flags based on dates
		returnFlag := "N"
		if receiptDate.Before(time.Date(1995, 6, 17, 0, 0, 0, 0, time.UTC)) {
			if rng.Intn(2) == 0 {
				returnFlag = "R"
			} else {
				returnFlag = "A"
			}
		}
		lineStatus := "O"
		if shipDate.Before(time.Date(1995, 6, 17, 0, 0, 0, 0, time.UTC)) {
			lineStatus = "F"
		}

		op := dsl.BatchWriteOperation{
			Method:      "POST",
			RelativeURL: "tpch/lineitem/pk-write",
			Body: dsl.PkWriteRequest{
				Filters: []dsl.Filter{
					{Column: "l_orderkey", Value: orderKey},
					{Column: "l_linenumber", Value: lineNumber},
				},
				WriteColumns: []dsl.WriteColumn{
					{Column: "l_partkey", Value: partKey},
					{Column: "l_suppkey", Value: suppKey},
					{Column: "l_quantity", Value: fmt.Sprintf("%.2f", quantity)},
					{Column: "l_extendedprice", Value: fmt.Sprintf("%.2f", extendedPrice)},
					{Column: "l_discount", Value: fmt.Sprintf("%.2f", discount)},
					{Column: "l_tax", Value: fmt.Sprintf("%.2f", tax)},
					{Column: "l_returnflag", Value: returnFlag},
					{Column: "l_linestatus", Value: lineStatus},
					{Column: "l_shipdate", Value: shipDate.Format("2006-01-02")},
					{Column: "l_commitdate", Value: commitDate.Format("2006-01-02")},
					{Column: "l_receiptdate", Value: receiptDate.Format("2006-01-02")},
					{Column: "l_shipinstruct", Value: tpchShipInstructs[rng.Intn(len(tpchShipInstructs))]},
					{Column: "l_shipmode", Value: tpchShipModes[rng.Intn(len(tpchShipModes))]},
					{Column: "l_comment", Value: generateComment(rng, 10, 43)},
				},
				OperationID: fmt.Sprintf("lineitem_%d_%d", orderKey, lineNumber),
			},
		}
		ops = append(ops, op)
	}
	return ops
}

// Helper functions for data generation

func generateComment(rng *rand.Rand, minLen, maxLen int) string {
	length := minLen + rng.Intn(maxLen-minLen+1)
	const charset = "abcdefghijklmnopqrstuvwxyz "
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rng.Intn(len(charset))]
	}
	return string(result)
}

func generateAddress(rng *rand.Rand) string {
	length := 10 + rng.Intn(31)
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ,."
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rng.Intn(len(charset))]
	}
	return string(result)
}

func generatePhone(rng *rand.Rand) string {
	countryCode := rng.Intn(25) + 10
	return fmt.Sprintf("%d-%03d-%03d-%04d", countryCode, rng.Intn(1000), rng.Intn(1000), rng.Intn(10000))
}

var partNameWords = []string{
	"almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
	"blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
	"cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick",
	"floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
	"hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
	"magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
	"navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder",
	"puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna",
	"sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet",
	"wheat", "white", "yellow",
}

func generatePartName(rng *rand.Rand) string {
	// Generate 5 random words
	words := make([]string, 5)
	used := make(map[int]bool)
	for i := 0; i < 5; i++ {
		for {
			idx := rng.Intn(len(partNameWords))
			if !used[idx] {
				used[idx] = true
				words[i] = partNameWords[idx]
				break
			}
		}
	}
	return fmt.Sprintf("%s %s %s %s %s", words[0], words[1], words[2], words[3], words[4])
}

// runLoadTPCH loads TPC-H data using the batchwrite endpoint
func (s *Shell) runLoadTPCH(scaleFactor int, numThreads int, batchSize int) error {
	if s.mysqlClient == nil {
		return fmt.Errorf("MySQL not connected. Cannot create TPC-H tables.")
	}
	if s.restClient == nil {
		return fmt.Errorf("REST API not connected. Cannot load TPC-H data.")
	}

	tables := GetTPCHTables()

	// Calculate total rows
	var totalRows int
	for _, table := range tables {
		totalRows += table.RowCount(scaleFactor)
	}

	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("Loading TPC-H data with scale factor %d", scaleFactor)))
	fmt.Println(ui.Info(fmt.Sprintf("Total rows to load: %d", totalRows)))
	fmt.Println(ui.Info(fmt.Sprintf("Using %d threads, batch size %d", numThreads, batchSize)))
	fmt.Println()

	// Create database and tables
	fmt.Println("Creating tpch database and tables...")
	_, _, err := s.mysqlClient.Execute("CREATE DATABASE IF NOT EXISTS tpch")
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	for _, table := range tables {
		ddl, ok := tpchTableDDL[table.Name]
		if !ok {
			return fmt.Errorf("no DDL for table %s", table.Name)
		}
		_, _, err = s.mysqlClient.Execute(ddl)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", table.Name, err)
		}
		fmt.Printf("   Created table tpch.%s\n", table.Name)
	}
	fmt.Println()

	// Load each table
	loadStart := time.Now()
	var totalRowsLoaded int64

	for _, table := range tables {
		rowCount := table.RowCount(scaleFactor)
		if rowCount == 0 {
			continue
		}

		fmt.Printf("Loading tpch.%s (%d rows)...\n", table.Name, rowCount)
		tableStart := time.Now()

		err := s.loadTPCHTable(table, scaleFactor, numThreads, batchSize, &totalRowsLoaded)
		if err != nil {
			return fmt.Errorf("failed to load table %s: %w", table.Name, err)
		}

		tableDuration := time.Since(tableStart)
		rowsPerSec := float64(rowCount) / tableDuration.Seconds()
		fmt.Printf("   Loaded %d rows in %v (%.0f rows/sec)\n", rowCount, tableDuration.Round(time.Millisecond), rowsPerSec)
	}

	totalDuration := time.Since(loadStart)
	totalRowsPerSec := float64(totalRowsLoaded) / totalDuration.Seconds()

	fmt.Println()
	fmt.Println(ui.Success(fmt.Sprintf("TPC-H load complete: %d rows in %v (%.0f rows/sec)",
		totalRowsLoaded, totalDuration.Round(time.Millisecond), totalRowsPerSec)))
	fmt.Println()

	return nil
}

// loadTPCHTable loads a single TPC-H table using multiple threads
func (s *Shell) loadTPCHTable(table TPCHTableInfo, scaleFactor int, numThreads int, batchSize int, totalRowsLoaded *int64) error {
	rowCount := table.RowCount(scaleFactor)
	if rowCount == 0 {
		return nil
	}

	// Create REST clients for each thread
	clients := make([]*client.RestClient, numThreads)
	for i := 0; i < numThreads; i++ {
		c, err := client.NewRestClientWithOptions(client.RestOptions{
			Host:   s.config.RDRSHost,
			Port:   s.config.RestPort,
			TLS:    s.config.RDRSTLS,
			APIKey: s.config.RDRSAPIKey,
		})
		if err != nil {
			for j := 0; j < i; j++ {
				clients[j].Close()
			}
			return fmt.Errorf("failed to create REST client: %w", err)
		}
		clients[i] = c
	}
	defer func() {
		for _, c := range clients {
			if c != nil {
				c.Close()
			}
		}
	}()

	// Split work among threads
	rowsPerThread := (rowCount + numThreads - 1) / numThreads
	errorCollector := NewErrorCollector()
	var completedRows int64
	var wg sync.WaitGroup

	// Progress reporting
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				rows := atomic.LoadInt64(&completedRows)
				pct := float64(rows) / float64(rowCount) * 100
				errs := errorCollector.Count()
				fmt.Printf("   Progress: %d/%d rows (%.1f%%), errors=%d\n", rows, rowCount, pct, errs)
			case <-stopProgress:
				return
			}
		}
	}()

	for t := 0; t < numThreads; t++ {
		startRow := t * rowsPerThread
		endRow := startRow + rowsPerThread
		if endRow > rowCount {
			endRow = rowCount
		}
		if startRow >= rowCount {
			continue
		}

		wg.Add(1)
		go func(threadID int, restClient *client.RestClient, start, end int) {
			defer wg.Done()

			// Each thread has its own RNG for deterministic generation
			rng := rand.New(rand.NewSource(int64(threadID * 1000000)))

			// Process in batches
			for batchStart := start; batchStart < end; batchStart += batchSize {
				batchEnd := batchStart + batchSize
				if batchEnd > end {
					batchEnd = end
				}

				ops := table.Generator(scaleFactor, batchStart, batchEnd, rng)
				if len(ops) == 0 {
					continue
				}

				request := dsl.BatchWriteRequest{
					Operations: ops,
				}

				_, _, err := restClient.Post("/" + APIVersion + "/batchwrite", request)
				if err != nil {
					errorCollector.Record(err)
				}

				rowsInserted := int64(len(ops))
				atomic.AddInt64(&completedRows, rowsInserted)
				atomic.AddInt64(totalRowsLoaded, rowsInserted)
			}
		}(t, clients[t], startRow, endRow)
	}

	wg.Wait()
	close(stopProgress)

	if errorCollector.Count() > 0 {
		errorCollector.PrintErrors()
	}

	return nil
}

// runDropTPCH drops all TPC-H tables and the tpch database
func (s *Shell) runDropTPCH() error {
	fmt.Println()
	fmt.Println("Dropping TPC-H tables and database...")

	tables := GetTPCHTables()
	// Drop tables in reverse order (due to potential FK relationships)
	for i := len(tables) - 1; i >= 0; i-- {
		table := tables[i]
		_, _, err := s.mysqlClient.Execute(fmt.Sprintf("DROP TABLE IF EXISTS tpch.%s", table.Name))
		if err != nil {
			fmt.Printf("   Warning: failed to drop table %s: %v\n", table.Name, err)
		} else {
			fmt.Printf("   Dropped table tpch.%s\n", table.Name)
		}
	}

	_, _, err := s.mysqlClient.Execute("DROP DATABASE IF EXISTS tpch")
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	fmt.Println(ui.Success("TPC-H database dropped"))
	fmt.Println()

	return nil
}
