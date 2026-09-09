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

import "github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"

// Table names of the data set (data_model.md §3).
const (
	TCustomers    = "customers_1"
	TCustomersStr = "customers_str_1"
	TProfiles     = "profiles_1"
	TRegions      = "regions_1"
	TCountries    = "countries_1"
	TMerchants    = "merchants_1"
	TTransactions = "transactions_1"
	TTxHash       = "transactions_hash_1"
	TTxStr        = "transactions_str_1"
	TSessions     = "sessions_1"
	TBalances     = "balances_1"
	TBalanceHist  = "balance_hist_1"
)

// SessionsTTLSeconds is the 100-year TTL of sessions_1: the TTL DDL path is
// exercised while nothing is ever purged.
const SessionsTTLSeconds int64 = 3153600000

func ttl(v int64) *int64 { return &v }

// customerFeatures returns the customers_1 feature list with the given key
// feature first (customers_str_1 swaps the key for a string).
func customerFeatures(key spec.Feature) []spec.Feature {
	return []spec.Feature{
		key,
		{Name: "region_id", Type: "int"},
		{Name: "tier", Type: "string"},
		{Name: "is_active", Type: "boolean"},
		{Name: "age", Type: "int"},
		{Name: "credit_score", Type: "int"},
		{Name: "credit", Type: "decimal(12,2)"},
		{Name: "signup_ts", Type: "timestamp"},
		{Name: "tags", Type: "array<string>", Complex: true},
	}
}

// txFeatures returns the transactions feature list with the given entity key.
func txFeatures(key spec.Feature) []spec.Feature {
	return []spec.Feature{
		key,
		{Name: "event_time", Type: "timestamp", Primary: true},
		{Name: "amount", Type: "bigint"},
		{Name: "fee", Type: "int"},
		{Name: "merchant_id", Type: "int"},
		{Name: "category", Type: "string"},
		{Name: "score", Type: "double"},
		{Name: "amount_dec", Type: "decimal(12,2)"},
		{Name: "flag", Type: "boolean"},
	}
}

// Schema is the feature-group set of the data set in load order (dimensions
// before the tables that reference them).  All feature groups belong to
// feature store 1 and are online enabled.
func Schema() []spec.FeatureGroup {
	bigKey := spec.Feature{Name: "customer_id", Type: "bigint", Primary: true}
	strKey := spec.Feature{Name: "customer_key", Type: "string", Primary: true}
	fgs := []spec.FeatureGroup{
		{
			Name: "countries", Version: 1, FeaturestoreID: 1,
			Features: []spec.Feature{
				{Name: "country_id", Type: "int", Primary: true},
				{Name: "country_name", Type: "string"},
				{Name: "continent", Type: "string"},
				{Name: "gdp", Type: "double"},
			},
		},
		{
			Name: "regions", Version: 1, FeaturestoreID: 1,
			Features: []spec.Feature{
				{Name: "region_id", Type: "int", Primary: true},
				{Name: "country_id", Type: "int"},
				{Name: "region_name", Type: "string"},
				{Name: "population", Type: "bigint"},
			},
		},
		{
			Name: "merchants", Version: 1, FeaturestoreID: 1,
			Features: []spec.Feature{
				{Name: "merchant_id", Type: "int", Primary: true},
				{Name: "mcc", Type: "int"},
				{Name: "name", Type: "string"},
			},
		},
		{
			Name: "customers", Version: 1, FeaturestoreID: 1,
			Online:   spec.OnlineConfig{SecondaryIndexes: [][]string{{"region_id"}}},
			Features: customerFeatures(bigKey),
		},
		{
			Name: "customers_str", Version: 1, FeaturestoreID: 1,
			Online:   spec.OnlineConfig{SecondaryIndexes: [][]string{{"region_id"}}},
			Features: customerFeatures(strKey),
		},
		{
			Name: "profiles", Version: 1, FeaturestoreID: 1,
			Features: []spec.Feature{
				{Name: "user_id", Type: "bigint", Primary: true},
				{Name: "region_id", Type: "int"},
				{Name: "tier", Type: "string"},
			},
		},
		{
			Name: "transactions", Version: 1, FeaturestoreID: 1, EventTime: "event_time",
			Online:   spec.OnlineConfig{SecondaryIndexes: [][]string{{"merchant_id"}}},
			Features: txFeatures(bigKey),
		},
		{
			Name: "transactions_hash", Version: 1, FeaturestoreID: 1, EventTime: "event_time",
			Online: spec.OnlineConfig{PrimaryKeyIndexType: "HASH",
				SecondaryIndexes: [][]string{{"merchant_id"}}},
			Features: txFeatures(bigKey),
		},
		{
			Name: "transactions_str", Version: 1, FeaturestoreID: 1, EventTime: "event_time",
			Online:   spec.OnlineConfig{SecondaryIndexes: [][]string{{"merchant_id"}}},
			Features: txFeatures(strKey),
		},
		{
			Name: "sessions", Version: 1, FeaturestoreID: 1, EventTime: "event_time",
			TTL: ttl(SessionsTTLSeconds),
			Features: []spec.Feature{
				bigKey,
				{Name: "event_time", Type: "timestamp", OnlineType: "timestamp(3)", Primary: true},
				{Name: "duration", Type: "int"},
				{Name: "pages", Type: "int"},
				{Name: "device", Type: "string"},
				{Name: "bytes", Type: "bigint"},
			},
		},
		{
			Name: "balances", Version: 1, FeaturestoreID: 1,
			Features: []spec.Feature{
				{Name: "account_id", Type: "bigint", Primary: true},
				{Name: "currency", Type: "string", Primary: true},
				{Name: "balance", Type: "decimal(18,2)"},
				{Name: "updated_ts", Type: "timestamp"},
				{Name: "overdraft", Type: "int"},
			},
		},
		{
			Name: "balance_hist", Version: 1, FeaturestoreID: 1, EventTime: "event_time",
			Features: []spec.Feature{
				{Name: "account_id", Type: "bigint", Primary: true},
				{Name: "currency", Type: "string", Primary: true},
				{Name: "event_time", Type: "timestamp", Primary: true},
				{Name: "delta", Type: "bigint"},
				{Name: "channel", Type: "string"},
			},
		},
	}
	fgs = append(fgs, EdgeSchema()...)
	return fgs
}

// SchemaMap returns the feature groups keyed by table name.
func SchemaMap() map[string]spec.FeatureGroup {
	m := map[string]spec.FeatureGroup{}
	for _, fg := range Schema() {
		m[fg.TableName()] = fg
	}
	return m
}
