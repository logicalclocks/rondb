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
	"strings"
	"testing"
	"time"
)

func TestClassSums(t *testing.T) {
	sum := func(v []int) int {
		s := 0
		for _, x := range v {
			s += x
		}
		return s
	}
	if got := sum(NTxClass[:]); got != 576 {
		t.Errorf("NTxClass sum = %d, want 576 (mean 36)", got)
	}
	if got := sum(NSessClass[:]); got != 53 {
		t.Errorf("NSessClass sum = %d, want 53", got)
	}
	if got := sum(NBalClass[:]); got != 18 {
		t.Errorf("NBalClass sum = %d, want 18", got)
	}
}

func TestScale(t *testing.T) {
	sc := NewScale(0.01)
	if sc.E != 1000 || sc.M != 100 || sc.A != 500 || sc.StrCustomers != 100 {
		t.Errorf("sf 0.01: %+v", sc)
	}
	sc = NewScale(1)
	if sc.E != 100000 || sc.M != 10000 || sc.A != 50000 || sc.StrCustomers != 10000 {
		t.Errorf("sf 1: %+v", sc)
	}
}

func TestTimeFormulas(t *testing.T) {
	// Customer 111: 300 rows, daily spacing ((111 div 16) mod 4 = 2), offset 0.
	if NTx(111) != 300 {
		t.Fatalf("NTx(111) = %d", NTx(111))
	}
	want := time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC)
	if got := TxEventTime(111, 7); !got.Equal(want) {
		t.Errorf("TxEventTime(111, 7) = %v, want %v (exactly 7 days before FS_NOW)", got, want)
	}
	// Customer 1: hourly spacing, offset 17 minutes.
	want = time.Date(2026, 5, 31, 22, 43, 0, 0, time.UTC)
	if got := TxEventTime(1, 1); !got.Equal(want) {
		t.Errorf("TxEventTime(1, 1) = %v, want %v", got, want)
	}
	// Sessions carry milliseconds: customer 3, row 1 -> 90 min + 3 s + 1 ms.
	if got := FormatTimestamp3(SessionEventTime(3, 1)); got != "2026-05-31 22:29:56.999" {
		t.Errorf("SessionEventTime(3, 1) = %s", got)
	}
}

func TestHops(t *testing.T) {
	if RegionOf(13).Valid {
		t.Error("customer 13 must have a NULL region")
	}
	if r := RegionOf(29); !r.Valid || r.V != 205 {
		t.Errorf("customer 29 must dangle to region 205, got %+v", r)
	}
	if r := RegionOf(1); !r.Valid || r.V != 2 {
		t.Errorf("customer 1 region = %+v, want 2", r)
	}
	if CountryOf(17).Valid {
		t.Error("region 17 must have a NULL country")
	}
	if c := CountryOf(19); !c.Valid || c.V != 42 {
		t.Errorf("region 19 must dangle to country 42, got %+v", c)
	}
	sc := NewScale(0.01)
	if m := MerchantOf(1, 10, sc); m.Valid {
		t.Error("(1+10) mod 11 = 0 must give a NULL merchant")
	}
	if m := MerchantOf(1, 22, sc); !m.Valid || m.V != 102 {
		t.Errorf("(1+22) mod 23 = 0 must dangle to M+1+(22 mod 3) = 102, got %+v", m)
	}
}

func TestValues(t *testing.T) {
	sc := NewScale(0.01)
	row := TxRow(5, 2, sc)
	if row.Amount != 100+(5*31+2*17)%900 || row.Fee != (5+2)%50-10 || row.Category != "Grocery" ||
		row.Score != 5+0.5 || FormatCents(row.AmountDecCents) != "1.89" || row.Flag != 1 {
		t.Errorf("TxRow(5,2) = %+v", row)
	}
	if CustomerKey(10) != "CUST-00000010" || CustomerKey(42) != "cust-00000042" {
		t.Error("CustomerKey")
	}
	c := CustomerRow(4)
	if c.Tier != "bronze" || c.IsActive != 1 || c.Age != 22 || FormatCents(c.CreditCents) != "5.50" {
		t.Errorf("CustomerRow(4) = %+v", c)
	}
	if FormatCents(-150) != "-1.50" || FormatCents(7) != "0.07" {
		t.Error("FormatCents")
	}
	if SQLString("it's") != "'it''s'" {
		t.Error("SQLString")
	}
}

func TestChecksumsSmallScale(t *testing.T) {
	// Hand-computed for E = 1000 (see data_model.md §4): 62 full cycles of
	// 16 customers (62 × 576) plus customers 993..1000 (classes 1..8).
	sc := NewScale(0.01)
	want := map[string]int64{
		TTransactions: 62*576 + (1 + 2 + 3 + 5 + 5 + 8 + 8 + 12),
		TTxHash:       62*576 + (1 + 2 + 3 + 5 + 5 + 8 + 8 + 12),
		TSessions:     125 * 53,
		TBalances:     166*6 + 2 + 3,
		TBalanceHist:  33*108 + 2 + 6 + 5 + 20 + 0,
		TCustomers:    1000,
		TRegions:      200,
		TCountries:    40,
		TMerchants:    100,
	}
	got := map[string]int64{}
	for _, c := range Checksums(sc, true) {
		got[c.Table] = c.Count
	}
	for table, w := range want {
		if got[table] != w {
			t.Errorf("%s count = %d, want %d", table, got[table], w)
		}
	}
	// transactions_str_1 covers customers 1..100: 6 cycles + classes 1..4.
	if got[TTxStr] != 6*576+(1+2+3+5) {
		t.Errorf("%s count = %d", TTxStr, got[TTxStr])
	}
}

func TestRowsMatchClasses(t *testing.T) {
	sc := NewScale(0.01)
	for _, tg := range Tables() {
		if tg.Table != TTransactions {
			continue
		}
		for _, c := range []int64{16, 17, 20, 111, 1000} {
			n := 0
			tg.Rows(sc, c, func(tuple string) {
				n++
				if !strings.HasPrefix(tuple, "(") || !strings.HasSuffix(tuple, ")") {
					t.Errorf("bad tuple %s", tuple)
				}
			})
			if n != NTx(c) {
				t.Errorf("customer %d: %d tuples, NTx = %d", c, n, NTx(c))
			}
		}
	}
}

func TestPick(t *testing.T) {
	sc := NewScale(0.01)
	has := func(ids []int64, v int64) bool {
		for _, x := range ids {
			if x == v {
				return true
			}
		}
		return false
	}
	if ids := Pick(ClassDayBounds, 20, sc); !has(ids, 108) || !has(ids, 111) {
		t.Errorf("ClassDayBounds must contain 108 and 111, got %v", ids)
	}
	if ids := Pick(ClassNoTx, 3, sc); len(ids) != 3 || ids[0] != 16 {
		t.Errorf("ClassNoTx = %v", ids)
	}
	if ids := Pick(ClassMissing, 2, sc); ids[0] != 1001 {
		t.Errorf("ClassMissing = %v", ids)
	}
	if ids := Pick(ClassNullCountry, 1, sc); len(ids) != 1 || ids[0] != 16 {
		t.Errorf("ClassNullCountry = %v (customer 16 -> region 17)", ids)
	}
}

func TestRenderMTR(t *testing.T) {
	o := MTROptions{DB: "test", Scale: NewScale(0.01), HashTwin: true}
	schema, err := RenderSchemaInc(o)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(schema, "CREATE TABLE IF NOT EXISTS `test`.`transactions_hash_1`(") ||
		!strings.Contains(schema, "PRIMARY KEY (`customer_id`,`event_time`) USING HASH") ||
		!strings.Contains(schema, "COMMENT='NDB_TABLE=TTL=3153600000@event_time,READ_BACKUP=1'") {
		t.Errorf("schema include:\n%s", schema)
	}
	data, err := RenderDataInc(o)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"SET time_zone = '+00:00';",
		"INSERT INTO `test`.`transactions_1` (`customer_id`, `event_time`, `amount`, `fee`, `merchant_id`, `category`, `score`, `amount_dec`, `flag`)",
		"WHERE c.n BETWEEN 1 AND 111",
		"INSERT INTO `test`.`transactions_hash_1`",
		"if ($fs_check != '35756,",
		"DROP TABLE _d, _seq, _seq300, _ntx, _nsess, _nbal, _spacing, _offset;",
	} {
		if !strings.Contains(data, want) {
			t.Errorf("data include misses %q", want)
		}
	}
	// A large scale must be refused for the include.
	if _, err := RenderDataInc(MTROptions{DB: "test", Scale: NewScale(3)}); err == nil {
		t.Error("sf 3 must be refused by the MTR renderer")
	}
}

func TestChecksumQuery(t *testing.T) {
	c := Checksum{Table: TTransactions, SumExpr: "amount", CntExpr: "merchant_id"}
	if got := c.Query("db"); got != "SELECT COUNT(*), SUM(amount), COUNT(merchant_id) FROM `db`.`transactions_1`" {
		t.Error(got)
	}
}
