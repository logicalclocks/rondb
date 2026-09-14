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

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

// TableGen describes how one table's rows are produced, twice: as Go value
// tuples (bench-scale loader, checksums) and as an INSERT ... SELECT over
// the MTR helper tables (MTR-scale include).  Both renderings implement the
// formulas of formulas.go; the checksum block pins that they agree.
type TableGen struct {
	Table   string
	FG      spec.FeatureGroup
	Columns []string
	// Entities is the number of top-level entities (customers, accounts,
	// regions, ...) whose rows the table holds; entity ids run 1..Entities.
	Entities func(sc Scale) int64
	// Mean is the mean number of rows per entity (chunk sizing).
	Mean float64
	// HashTwin marks the optional transactions_hash_1 copy.
	HashTwin bool
	// Fixed marks a small literal-row table (data_model.md §11): Rows emits
	// every row for the single pseudo entity 1 and Select is nil; both
	// loaders insert the same tuples with INSERT ... VALUES.
	Fixed bool
	// Rows renders every row of entity e as a SQL value tuple "(v1, v2, ...)".
	Rows func(sc Scale, e int64, emit func(tuple string))
	// Select renders the SELECT part of the MTR INSERT ... SELECT for the
	// entity range [lo, hi]; db is the database of the (already loaded)
	// tables it may read from.
	Select func(sc Scale, db string, lo, hi int64) string
}

// ColumnList renders "(`c1`, `c2`, ...)".
func (t TableGen) ColumnList() string {
	quoted := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		quoted[i] = "`" + c + "`"
	}
	return "(" + strings.Join(quoted, ", ") + ")"
}

// ChunkEntities is the number of entities per INSERT statement so that a
// statement inserts about targetRows rows (MTR's MaxNoOfConcurrentOperations
// is 10000; the include targets 4000).
func (t TableGen) ChunkEntities(targetRows int) int64 {
	n := int64(float64(targetRows) / t.Mean)
	if n < 1 {
		n = 1
	}
	return n
}

func tuple(vals ...string) string { return "(" + strings.Join(vals, ", ") + ")" }

func itoa(v int64) string { return fmt.Sprintf("%d", v) }

// SQL fragments shared by the renderers.
const (
	sqlRegionOf   = "CASE WHEN MOD(%[1]s,13)=0 THEN NULL WHEN MOD(%[1]s,29)=0 THEN 201 + MOD(%[1]s,5) ELSE MOD(%[1]s,200)+1 END"
	sqlTier       = "ELT(MOD(%s,4)+1,'bronze','silver','gold','platinum')"
	sqlCustKey    = "CONCAT(IF(MOD(%[1]s,10)=0,'CUST-','cust-'), LPAD(%[1]s, 8, '0'))"
	sqlCategories = "'grocery','Grocery','fuel','travel','Travel','online'"
)

func customerSelect(keyExpr string) string {
	return "SELECT " + keyExpr + ",\n" +
		"       " + fmt.Sprintf(sqlRegionOf, "n") + ",\n" +
		"       " + fmt.Sprintf(sqlTier, "n") + ",\n" +
		"       IF(MOD(n,5)=0, 0, 1),\n" +
		"       18 + MOD(n,70),\n" +
		"       300 + MOD(n*37, 551),\n" +
		"       n*1.25 + 0.50,\n" +
		"       DATE_SUB('" + FSNowString + "', INTERVAL (n*10) MINUTE),\n" +
		"       UNHEX('0102')\n" +
		"FROM _seq WHERE n BETWEEN %d AND %d"
}

func customerTuple(key string, row Customer) string {
	return tuple(key, SQLOptInt(row.RegionID), SQLString(row.Tier), itoa(int64(row.IsActive)),
		itoa(int64(row.Age)), itoa(int64(row.CreditScore)), FormatCents(row.CreditCents),
		SQLString(FormatTimestamp(row.SignupTS)), "UNHEX('0102')")
}

func txSelect(keyExpr string, sc Scale) string {
	return "SELECT " + keyExpr + ",\n" +
		"       DATE_SUB('" + FSNowString + "', INTERVAL (i.n * sp.s + off.o) SECOND),\n" +
		"       100 + MOD(c.n*31 + i.n*17, 900),\n" +
		"       MOD(c.n + i.n, 50) - 10,\n" +
		fmt.Sprintf("       CASE WHEN MOD(c.n + i.n, 11)=0 THEN NULL WHEN MOD(c.n + i.n, 23)=0 THEN %d + 1 + MOD(i.n,3) ELSE MOD(c.n*7 + i.n, %d) + 1 END,\n", sc.M, sc.M) +
		"       ELT(MOD(c.n + i.n, 6)+1, " + sqlCategories + "),\n" +
		"       MOD(c.n, 97) + i.n * 0.25,\n" +
		"       MOD(c.n*31 + i.n*17, 90000) / 100,\n" +
		"       MOD(c.n + i.n, 2)\n" +
		"FROM _seq AS c\n" +
		"JOIN _ntx ON _ntx.k = MOD(c.n, 16)\n" +
		"JOIN _seq300 AS i ON i.n <= _ntx.n\n" +
		"JOIN _spacing AS sp ON sp.k = MOD(c.n DIV 16, 4)\n" +
		"JOIN _offset AS off ON off.k = MOD(c.n, 3)\n" +
		"WHERE c.n BETWEEN %d AND %d"
}

func txTuple(key string, row Tx) string {
	return tuple(key, SQLString(FormatTimestamp(row.EventTime)), itoa(row.Amount), itoa(int64(row.Fee)),
		SQLOptInt(row.MerchantID), SQLString(row.Category), SQLDouble(row.Score),
		FormatCents(row.AmountDecCents), itoa(int64(row.Flag)))
}

// Tables returns the table generators in load order.
func Tables() []TableGen {
	fgs := SchemaMap()
	mk := func(table string) (spec.FeatureGroup, []string) {
		fg := fgs[table]
		var cols []string
		for _, f := range fg.Features {
			if !f.OfflineOnly {
				cols = append(cols, f.Name)
			}
		}
		return fg, cols
	}
	var out []TableGen

	fg, cols := mk(TCountries)
	out = append(out, TableGen{
		Table: TCountries, FG: fg, Columns: cols, Mean: 1,
		Entities: func(sc Scale) int64 { return CountryCount },
		Rows: func(sc Scale, k int64, emit func(string)) {
			row := CountryRow(k)
			emit(tuple(itoa(row.ID), SQLString(row.Name), SQLString(row.Continent), SQLDouble(row.GDP)))
		},
		Select: func(sc Scale, db string, lo, hi int64) string {
			return fmt.Sprintf("SELECT n, CONCAT('Country ', n),\n"+
				"       ELT(MOD(n,5)+1,'Europe','Asia','Africa','America','Oceania'),\n"+
				"       n*1000 + 0.25\n"+
				"FROM _seq WHERE n BETWEEN %d AND %d", lo, hi)
		},
	})

	fg, cols = mk(TRegions)
	out = append(out, TableGen{
		Table: TRegions, FG: fg, Columns: cols, Mean: 1,
		Entities: func(sc Scale) int64 { return RegionCount },
		Rows: func(sc Scale, r int64, emit func(string)) {
			row := RegionRow(r)
			emit(tuple(itoa(row.ID), SQLOptInt(row.CountryID), SQLString(row.Name), itoa(row.Population)))
		},
		Select: func(sc Scale, db string, lo, hi int64) string {
			return fmt.Sprintf("SELECT n,\n"+
				"       CASE WHEN MOD(n,17)=0 THEN NULL WHEN MOD(n,19)=0 THEN 41 + MOD(n,3) ELSE MOD(n-1,40)+1 END,\n"+
				"       CONCAT('Region ', n), n*12345\n"+
				"FROM _seq WHERE n BETWEEN %d AND %d", lo, hi)
		},
	})

	fg, cols = mk(TMerchants)
	out = append(out, TableGen{
		Table: TMerchants, FG: fg, Columns: cols, Mean: 1,
		Entities: func(sc Scale) int64 { return sc.M },
		Rows: func(sc Scale, m int64, emit func(string)) {
			row := MerchantRow(m)
			emit(tuple(itoa(row.ID), itoa(int64(row.MCC)), SQLString(row.Name)))
		},
		Select: func(sc Scale, db string, lo, hi int64) string {
			return fmt.Sprintf("SELECT n, 5000 + MOD(n,100), CONCAT('Merchant ', n)\n"+
				"FROM _seq WHERE n BETWEEN %d AND %d", lo, hi)
		},
	})

	fg, cols = mk(TCustomers)
	out = append(out, TableGen{
		Table: TCustomers, FG: fg, Columns: cols, Mean: 1,
		Entities: func(sc Scale) int64 { return sc.E },
		Rows: func(sc Scale, c int64, emit func(string)) {
			emit(customerTuple(itoa(c), CustomerRow(c)))
		},
		Select: func(sc Scale, db string, lo, hi int64) string {
			return fmt.Sprintf(customerSelect("n"), lo, hi)
		},
	})

	fg, cols = mk(TCustomersStr)
	out = append(out, TableGen{
		Table: TCustomersStr, FG: fg, Columns: cols, Mean: 1,
		Entities: func(sc Scale) int64 { return sc.E },
		Rows: func(sc Scale, c int64, emit func(string)) {
			emit(customerTuple(SQLString(CustomerKey(c)), CustomerRow(c)))
		},
		Select: func(sc Scale, db string, lo, hi int64) string {
			return fmt.Sprintf(customerSelect(fmt.Sprintf(sqlCustKey, "n")), lo, hi)
		},
	})

	fg, cols = mk(TProfiles)
	out = append(out, TableGen{
		Table: TProfiles, FG: fg, Columns: cols, Mean: 1,
		Entities: func(sc Scale) int64 { return sc.E },
		Rows: func(sc Scale, c int64, emit func(string)) {
			row := CustomerRow(c)
			emit(tuple(itoa(c), SQLOptInt(row.RegionID), SQLString(row.Tier)))
		},
		Select: func(sc Scale, db string, lo, hi int64) string {
			return fmt.Sprintf("SELECT n, "+fmt.Sprintf(sqlRegionOf, "n")+", "+fmt.Sprintf(sqlTier, "n")+"\n"+
				"FROM _seq WHERE n BETWEEN %d AND %d", lo, hi)
		},
	})

	fg, cols = mk(TTransactions)
	out = append(out, TableGen{
		Table: TTransactions, FG: fg, Columns: cols, Mean: 36,
		Entities: func(sc Scale) int64 { return sc.E },
		Rows: func(sc Scale, c int64, emit func(string)) {
			for i := 1; i <= NTx(c); i++ {
				emit(txTuple(itoa(c), TxRow(c, i, sc)))
			}
		},
		Select: func(sc Scale, db string, lo, hi int64) string {
			return fmt.Sprintf(txSelect("c.n", sc), lo, hi)
		},
	})

	fg, cols = mk(TTxHash)
	out = append(out, TableGen{
		Table: TTxHash, FG: fg, Columns: cols, Mean: 36, HashTwin: true,
		Entities: func(sc Scale) int64 { return sc.E },
		Rows: func(sc Scale, c int64, emit func(string)) {
			for i := 1; i <= NTx(c); i++ {
				emit(txTuple(itoa(c), TxRow(c, i, sc)))
			}
		},
		Select: func(sc Scale, db string, lo, hi int64) string {
			return fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE customer_id BETWEEN %d AND %d", db, TTransactions, lo, hi)
		},
	})

	fg, cols = mk(TTxStr)
	out = append(out, TableGen{
		Table: TTxStr, FG: fg, Columns: cols, Mean: 36,
		Entities: func(sc Scale) int64 { return sc.StrCustomers },
		Rows: func(sc Scale, c int64, emit func(string)) {
			for i := 1; i <= NTx(c); i++ {
				emit(txTuple(SQLString(CustomerKey(c)), TxRow(c, i, sc)))
			}
		},
		Select: func(sc Scale, db string, lo, hi int64) string {
			return fmt.Sprintf(txSelect(fmt.Sprintf(sqlCustKey, "c.n"), sc), lo, hi)
		},
	})

	fg, cols = mk(TSessions)
	out = append(out, TableGen{
		Table: TSessions, FG: fg, Columns: cols, Mean: 6.625,
		Entities: func(sc Scale) int64 { return sc.E },
		Rows: func(sc Scale, c int64, emit func(string)) {
			for i := 1; i <= NSess(c); i++ {
				row := SessionRow(c, i)
				emit(tuple(itoa(c), SQLString(FormatTimestamp3(row.EventTime)), itoa(int64(row.Duration)),
					itoa(int64(row.Pages)), SQLString(row.Device), itoa(row.Bytes)))
			}
		},
		Select: func(sc Scale, db string, lo, hi int64) string {
			return fmt.Sprintf("SELECT c.n,\n"+
				"       DATE_SUB('"+FSNowString+".000', INTERVAL ((i.n*5400000 + MOD(c.n,60)*1000 + MOD(i.n,1000)) * 1000) MICROSECOND),\n"+
				"       30 + MOD(c.n*13 + i.n*7, 3600),\n"+
				"       1 + MOD(c.n + i.n, 40),\n"+
				"       ELT(MOD(c.n + i.n, 4)+1, 'ios','android','web','WEB'),\n"+
				"       MOD(c.n*1009 + i.n*4093, 10000000)\n"+
				"FROM _seq AS c\n"+
				"JOIN _nsess ON _nsess.k = MOD(c.n, 8)\n"+
				"JOIN _seq300 AS i ON i.n <= _nsess.n\n"+
				"WHERE c.n BETWEEN %d AND %d", lo, hi)
		},
	})

	fg, cols = mk(TBalances)
	out = append(out, TableGen{
		Table: TBalances, FG: fg, Columns: cols, Mean: 2,
		Entities: func(sc Scale) int64 { return sc.A },
		Rows: func(sc Scale, a int64, emit func(string)) {
			for j := 0; j < NCurrencies(a); j++ {
				row := BalanceRow(a, j)
				emit(tuple(itoa(a), SQLString(row.Currency), FormatCents(row.BalanceCents),
					SQLString(FormatTimestamp(row.UpdatedTS)), itoa(int64(row.Overdraft))))
			}
		},
		Select: func(sc Scale, db string, lo, hi int64) string {
			return fmt.Sprintf("SELECT a.n, ELT(j.d+1,'USD','EUR','SEK'), a.n*3.75 + j.d,\n"+
				"       DATE_SUB('"+FSNowString+"', INTERVAL a.n MINUTE), MOD(a.n,4)*500\n"+
				"FROM _seq AS a JOIN _d AS j ON j.d <= MOD(a.n, 3)\n"+
				"WHERE a.n BETWEEN %d AND %d", lo, hi)
		},
	})

	fg, cols = mk(TBalanceHist)
	out = append(out, TableGen{
		Table: TBalanceHist, FG: fg, Columns: cols, Mean: 7.2,
		Entities: func(sc Scale) int64 { return sc.A },
		Rows: func(sc Scale, a int64, emit func(string)) {
			for j := 0; j < NCurrencies(a); j++ {
				for i := 1; i <= NBal(a); i++ {
					row := BalanceHistRow(a, j, i)
					emit(tuple(itoa(a), SQLString(row.Currency), SQLString(FormatTimestamp(row.EventTime)),
						itoa(row.Delta), SQLString(row.Channel)))
				}
			}
		},
		Select: func(sc Scale, db string, lo, hi int64) string {
			return fmt.Sprintf("SELECT a.n, ELT(j.d+1,'USD','EUR','SEK'),\n"+
				"       DATE_SUB('"+FSNowString+"', INTERVAL (i.n*43200 + MOD(a.n,7)*3600) SECOND),\n"+
				"       MOD(a.n + i.n*7, 200) - 100,\n"+
				"       ELT(MOD(a.n + i.n, 3)+1, 'card','wire','atm')\n"+
				"FROM _seq AS a JOIN _d AS j ON j.d <= MOD(a.n, 3)\n"+
				"JOIN _nbal ON _nbal.k = MOD(a.n, 5)\n"+
				"JOIN _seq300 AS i ON i.n <= _nbal.n\n"+
				"WHERE a.n BETWEEN %d AND %d", lo, hi)
		},
	})
	out = append(out, EdgeTables()...)
	return out
}
