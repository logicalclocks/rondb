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

import "fmt"

// Checksum pins the content of one table: the row count plus one sum and one
// non-NULL count that the SQL renderer and the Go loader must both produce.
// The expressions are chosen to be exact integers on every engine.
type Checksum struct {
	Table    string
	Count    int64
	SumExpr  string // SQL expression summed (empty: none)
	Sum      int64
	CntExpr  string // SQL column counted with COUNT(col) (empty: none)
	CntValue int64
}

// Query returns the SELECT that produces (count, sum, cnt) for the table.
func (c Checksum) Query(db string) string {
	sum, cnt := "0", "0"
	if c.SumExpr != "" {
		sum = "SUM(" + c.SumExpr + ")"
	}
	if c.CntExpr != "" {
		cnt = "COUNT(" + c.CntExpr + ")"
	}
	return fmt.Sprintf("SELECT COUNT(*), %s, %s FROM `%s`.`%s`", sum, cnt, db, c.Table)
}

// Checksums computes the expected checksums of every table at the scale by
// regenerating the rows in memory.
func Checksums(sc Scale, hashTwin bool) []Checksum {
	var out []Checksum

	cust := Checksum{Table: TCustomers, CntExpr: "region_id", SumExpr: "age"}
	custStr := Checksum{Table: TCustomersStr, CntExpr: "region_id", SumExpr: "age"}
	prof := Checksum{Table: TProfiles, CntExpr: "region_id"}
	for c := int64(1); c <= sc.E; c++ {
		row := CustomerRow(c)
		cust.Count++
		cust.Sum += int64(row.Age)
		if row.RegionID.Valid {
			cust.CntValue++
		}
	}
	custStr.Count, custStr.Sum, custStr.CntValue = cust.Count, cust.Sum, cust.CntValue
	prof.Count, prof.CntValue = cust.Count, cust.CntValue
	out = append(out, cust, custStr, prof)

	reg := Checksum{Table: TRegions, SumExpr: "population", CntExpr: "country_id"}
	for r := int64(1); r <= RegionCount; r++ {
		row := RegionRow(r)
		reg.Count++
		reg.Sum += row.Population
		if row.CountryID.Valid {
			reg.CntValue++
		}
	}
	out = append(out, reg)
	out = append(out, Checksum{Table: TCountries, Count: CountryCount})
	out = append(out, Checksum{Table: TMerchants, Count: sc.M})

	tx := Checksum{Table: TTransactions, SumExpr: "amount", CntExpr: "merchant_id"}
	txStr := Checksum{Table: TTxStr, SumExpr: "amount", CntExpr: "merchant_id"}
	for c := int64(1); c <= sc.E; c++ {
		for i := 1; i <= NTx(c); i++ {
			row := TxRow(c, i, sc)
			tx.Count++
			tx.Sum += row.Amount
			if row.MerchantID.Valid {
				tx.CntValue++
			}
			if c <= sc.StrCustomers {
				txStr.Count++
				txStr.Sum += row.Amount
				if row.MerchantID.Valid {
					txStr.CntValue++
				}
			}
		}
	}
	out = append(out, tx)
	if hashTwin {
		h := tx
		h.Table = TTxHash
		out = append(out, h)
	}
	out = append(out, txStr)

	sess := Checksum{Table: TSessions, SumExpr: "duration", CntExpr: "device"}
	for c := int64(1); c <= sc.E; c++ {
		for i := 1; i <= NSess(c); i++ {
			row := SessionRow(c, i)
			sess.Count++
			sess.Sum += int64(row.Duration)
			sess.CntValue++
		}
	}
	out = append(out, sess)

	bal := Checksum{Table: TBalances, SumExpr: "overdraft", CntExpr: "currency"}
	hist := Checksum{Table: TBalanceHist, SumExpr: "delta", CntExpr: "channel"}
	for a := int64(1); a <= sc.A; a++ {
		for j := 0; j < NCurrencies(a); j++ {
			row := BalanceRow(a, j)
			bal.Count++
			bal.Sum += int64(row.Overdraft)
			bal.CntValue++
			for i := 1; i <= NBal(a); i++ {
				h := BalanceHistRow(a, j, i)
				hist.Count++
				hist.Sum += h.Delta
				hist.CntValue++
			}
		}
	}
	out = append(out, bal, hist)
	return out
}

// ChecksumFor returns the checksum of one table.
func ChecksumFor(table string, sc Scale) (Checksum, bool) {
	for _, c := range Checksums(sc, true) {
		if c.Table == table {
			return c, true
		}
	}
	return Checksum{}, false
}
