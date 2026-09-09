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

// Package data is the deterministic feature-store data set of the RonSQL
// feature-store test framework (RONDB-1121): the feature-group schema, the
// closed-form value formulas, the row-count classes, the checksums, the
// MTR-scale SQL renderer and the bench-scale loader.  Every value is a
// function of the row coordinates (entity id, row index) and of the scale
// factor; nothing is random, and the Go formulas and their SQL renderings
// must produce identical rows (pinned by the checksum blocks).
//
// Design: storage/ndb/claude_files/fs_ronsql/data_model.md.
package data

import (
	"fmt"
	"math"
	"time"
)

// FSNow is the single reference clock of the data set: every event time is
// FSNow - age(entity, row).  Windowed statements bind FSNow - window on
// both engines; benchmarks use it too.
var FSNow = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

// FSNowString is FSNow as a MySQL literal (UTC).
const FSNowString = "2026-06-01 00:00:00"

// Base sizes (data_model.md §2, §3).
const (
	BaseCustomers = 100000 // customers at scale factor 1
	BaseMerchants = 10000  // merchants at scale factor 1
	MinMerchants  = 100
	RegionCount   = 200
	CountryCount  = 40
	// MaxRowsPerEntity is the largest row-count class (transactions of a
	// c mod 16 = 15 customer); the SQL renderer's row helper table covers it.
	MaxRowsPerEntity = 300
)

// Row-count classes (data_model.md §4).
var (
	NTxClass   = [16]int{0, 1, 2, 3, 5, 5, 8, 8, 12, 12, 20, 20, 30, 50, 100, 300}
	NSessClass = [8]int{0, 1, 2, 4, 6, 8, 12, 20}
	NBalClass  = [5]int{0, 1, 2, 5, 10}
	// SpacingSeconds by (c div 16) mod 4: 1 hour, 6 hours, 1 day, 7 days.
	SpacingSeconds = [4]int64{3600, 21600, 86400, 604800}
	// OffsetSeconds by c mod 3: 0, 17 minutes, 41 minutes.
	OffsetSeconds = [3]int64{0, 1020, 2460}
)

// Value alphabets (data_model.md §5).
var (
	Tiers      = [4]string{"bronze", "silver", "gold", "platinum"}
	Categories = [6]string{"grocery", "Grocery", "fuel", "travel", "Travel", "online"}
	Devices    = [4]string{"ios", "android", "web", "WEB"}
	Continents = [5]string{"Europe", "Asia", "Africa", "America", "Oceania"}
	Currencies = [3]string{"USD", "EUR", "SEK"}
	Channels   = [3]string{"card", "wire", "atm"}
)

// Scale is the resolved size of one data set.
type Scale struct {
	SF           float64
	E            int64 // customers (customers_1, customers_str_1, profiles_1)
	M            int64 // merchants
	A            int64 // accounts (balances_1, balance_hist_1) = E/2
	StrCustomers int64 // customers with transactions_str_1 rows = E/10
}

// NewScale resolves a scale factor: E = 100000 × sf, M = max(100, 10000 × sf).
func NewScale(sf float64) Scale {
	e := int64(math.Round(BaseCustomers * sf))
	if e < 16 {
		e = 16
	}
	m := int64(math.Round(BaseMerchants * sf))
	if m < MinMerchants {
		m = MinMerchants
	}
	return Scale{SF: sf, E: e, M: m, A: e / 2, StrCustomers: e / 10}
}

// OptInt is a nullable integer column value.
type OptInt struct {
	V     int64
	Valid bool
}

func some(v int64) OptInt { return OptInt{V: v, Valid: true} }

var null = OptInt{}

// ---- row-count and time helpers -------------------------------------------

// NTx is the number of transactions_1 rows of customer c.
func NTx(c int64) int { return NTxClass[c%16] }

// NSess is the number of sessions_1 rows of customer c.
func NSess(c int64) int { return NSessClass[c%8] }

// NBal is the number of balance_hist_1 rows per (account, currency) of account a.
func NBal(a int64) int { return NBalClass[a%5] }

// NCurrencies is the number of balances_1 currencies of account a: 1 + a mod 3.
func NCurrencies(a int64) int { return int(1 + a%3) }

// TxAgeSeconds is age(c, i) = i × spacing(c) + offset(c) (data_model.md §5.1).
func TxAgeSeconds(c int64, i int) int64 {
	return int64(i)*SpacingSeconds[(c/16)%4] + OffsetSeconds[c%3]
}

// TxEventTime is FSNow - age(c, i).
func TxEventTime(c int64, i int) time.Time {
	return FSNow.Add(-time.Duration(TxAgeSeconds(c, i)) * time.Second)
}

// SessionAgeMillis is i × 90 min + (c mod 60) s + (i mod 1000) ms.
func SessionAgeMillis(c int64, i int) int64 {
	return int64(i)*5400000 + (c%60)*1000 + int64(i%1000)
}

// SessionEventTime is FSNow - SessionAgeMillis (TIMESTAMP(3)).
func SessionEventTime(c int64, i int) time.Time {
	return FSNow.Add(-time.Duration(SessionAgeMillis(c, i)) * time.Millisecond)
}

// BalanceHistAgeSeconds is i × 12 h + (a mod 7) h.
func BalanceHistAgeSeconds(a int64, i int) int64 { return int64(i)*43200 + (a%7)*3600 }

// BalanceHistEventTime is FSNow - BalanceHistAgeSeconds.
func BalanceHistEventTime(a int64, i int) time.Time {
	return FSNow.Add(-time.Duration(BalanceHistAgeSeconds(a, i)) * time.Second)
}

// ---- entity and dimension rows ---------------------------------------------

// Customer is one customers_1 / customers_str_1 row.
type Customer struct {
	ID          int64
	RegionID    OptInt
	Tier        string
	IsActive    int
	Age         int
	CreditScore int
	CreditCents int64 // credit DECIMAL(12,2) in cents
	SignupTS    time.Time
	Tags        []byte
}

// RegionOf is the region hop of customer c: NULL when c mod 13 = 0, dangling
// (201 + c mod 5) when c mod 29 = 0, else (c mod 200) + 1.
func RegionOf(c int64) OptInt {
	switch {
	case c%13 == 0:
		return null
	case c%29 == 0:
		return some(RegionCount + 1 + c%5)
	default:
		return some(c%RegionCount + 1)
	}
}

// CustomerRow is customers_1(c).
func CustomerRow(c int64) Customer {
	active := 1
	if c%5 == 0 {
		active = 0
	}
	return Customer{
		ID:          c,
		RegionID:    RegionOf(c),
		Tier:        Tiers[c%4],
		IsActive:    active,
		Age:         int(18 + c%70),
		CreditScore: int(300 + (c*37)%551),
		CreditCents: c*125 + 50,
		SignupTS:    FSNow.Add(-time.Duration(c) * 10 * time.Minute),
		Tags:        []byte{0x01, 0x02},
	}
}

// CustomerKey is the string key of customer c: cust-00000042, or CUST-…
// for every 10th customer (collation probe).
func CustomerKey(c int64) string {
	if c%10 == 0 {
		return fmt.Sprintf("CUST-%08d", c)
	}
	return fmt.Sprintf("cust-%08d", c)
}

// Region is one regions_1 row.
type Region struct {
	ID         int64
	CountryID  OptInt
	Name       string
	Population int64
}

// CountryOf is the country hop of region r: NULL when r mod 17 = 0, dangling
// (41 + r mod 3) when r mod 19 = 0, else ((r-1) mod 40) + 1.
func CountryOf(r int64) OptInt {
	switch {
	case r%17 == 0:
		return null
	case r%19 == 0:
		return some(CountryCount + 1 + r%3)
	default:
		return some((r-1)%CountryCount + 1)
	}
}

// RegionRow is regions_1(r).
func RegionRow(r int64) Region {
	return Region{ID: r, CountryID: CountryOf(r), Name: fmt.Sprintf("Region %d", r), Population: r * 12345}
}

// Country is one countries_1 row.
type Country struct {
	ID        int64
	Name      string
	Continent string
	GDP       float64
}

// CountryRow is countries_1(k).
func CountryRow(k int64) Country {
	return Country{ID: k, Name: fmt.Sprintf("Country %d", k), Continent: Continents[k%5], GDP: float64(k)*1000 + 0.25}
}

// Merchant is one merchants_1 row.
type Merchant struct {
	ID   int64
	MCC  int
	Name string
}

// MerchantRow is merchants_1(m).
func MerchantRow(m int64) Merchant {
	return Merchant{ID: m, MCC: int(5000 + m%100), Name: fmt.Sprintf("Merchant %d", m)}
}

// ---- history rows -----------------------------------------------------------

// Tx is one transactions_1 / transactions_hash_1 / transactions_str_1 row.
type Tx struct {
	CustomerID     int64
	EventTime      time.Time
	Amount         int64
	Fee            int
	MerchantID     OptInt
	Category       string
	Score          float64
	AmountDecCents int64 // amount_dec DECIMAL(12,2) in cents
	Flag           int
}

// MerchantOf is the merchant hop of transaction (c, i): NULL when
// (c+i) mod 11 = 0, dangling (M + 1 + i mod 3) when (c+i) mod 23 = 0, else
// ((c×7 + i) mod M) + 1.
func MerchantOf(c int64, i int, sc Scale) OptInt {
	s := c + int64(i)
	switch {
	case s%11 == 0:
		return null
	case s%23 == 0:
		return some(sc.M + 1 + int64(i%3))
	default:
		return some((c*7+int64(i))%sc.M + 1)
	}
}

// TxRow is transactions_1(c, i), i = 1..NTx(c); i = 1 is the newest row.
func TxRow(c int64, i int, sc Scale) Tx {
	ii := int64(i)
	return Tx{
		CustomerID:     c,
		EventTime:      TxEventTime(c, i),
		Amount:         100 + (c*31+ii*17)%900,
		Fee:            int((c+ii)%50 - 10),
		MerchantID:     MerchantOf(c, i, sc),
		Category:       Categories[(c+ii)%6],
		Score:          float64(c%97) + float64(i)*0.25,
		AmountDecCents: (c*31 + ii*17) % 90000,
		Flag:           int((c + ii) % 2),
	}
}

// Session is one sessions_1 row.
type Session struct {
	CustomerID int64
	EventTime  time.Time // millisecond precision
	Duration   int
	Pages      int
	Device     string
	Bytes      int64
}

// SessionRow is sessions_1(c, i), i = 1..NSess(c).
func SessionRow(c int64, i int) Session {
	ii := int64(i)
	return Session{
		CustomerID: c,
		EventTime:  SessionEventTime(c, i),
		Duration:   int(30 + (c*13+ii*7)%3600),
		Pages:      int(1 + (c+ii)%40),
		Device:     Devices[(c+ii)%4],
		Bytes:      (c*1009 + ii*4093) % 10000000,
	}
}

// Balance is one balances_1 row.
type Balance struct {
	AccountID    int64
	Currency     string
	BalanceCents int64 // balance DECIMAL(18,2) in cents
	UpdatedTS    time.Time
	Overdraft    int
}

// BalanceRow is balances_1(a, j), j = 0..NCurrencies(a)-1.
func BalanceRow(a int64, j int) Balance {
	return Balance{
		AccountID:    a,
		Currency:     Currencies[j],
		BalanceCents: a*375 + int64(j)*100,
		UpdatedTS:    FSNow.Add(-time.Duration(a) * time.Minute),
		Overdraft:    int((a % 4) * 500),
	}
}

// BalanceHist is one balance_hist_1 row.
type BalanceHist struct {
	AccountID int64
	Currency  string
	EventTime time.Time
	Delta     int64
	Channel   string
}

// BalanceHistRow is balance_hist_1(a, j, i), i = 1..NBal(a).
func BalanceHistRow(a int64, j int, i int) BalanceHist {
	ii := int64(i)
	return BalanceHist{
		AccountID: a,
		Currency:  Currencies[j],
		EventTime: BalanceHistEventTime(a, i),
		Delta:     (a+ii*7)%200 - 100,
		Channel:   Channels[(a+ii)%3],
	}
}

// ---- literal rendering -------------------------------------------------------

// FormatCents renders a cents amount as a DECIMAL literal ("12.30").
func FormatCents(cents int64) string {
	sign := ""
	if cents < 0 {
		sign = "-"
		cents = -cents
	}
	return fmt.Sprintf("%s%d.%02d", sign, cents/100, cents%100)
}

// FormatTimestamp renders t as 'YYYY-MM-DD HH:MM:SS' (UTC).
func FormatTimestamp(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05") }

// FormatTimestamp3 renders t as 'YYYY-MM-DD HH:MM:SS.fff' (UTC).
func FormatTimestamp3(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000") }

// SQLString quotes s as a SQL string literal (” doubling; the alphabet has
// no backslashes or control characters).
func SQLString(s string) string {
	var b []byte
	b = append(b, '\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			b = append(b, '\'')
		}
		b = append(b, s[i])
	}
	b = append(b, '\'')
	return string(b)
}

// SQLOptInt renders an OptInt as a literal or NULL.
func SQLOptInt(v OptInt) string {
	if !v.Valid {
		return "NULL"
	}
	return fmt.Sprintf("%d", v.V)
}

// SQLDouble renders a double literal with enough digits to round-trip.
func SQLDouble(f float64) string { return fmt.Sprintf("%g", f) }
