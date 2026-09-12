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

package ddl

import (
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

func TestOnlineType(t *testing.T) {
	cases := []struct {
		f    spec.Feature
		want string
	}{
		{spec.Feature{Type: "bigint"}, "bigint"},
		{spec.Feature{Type: "int"}, "int"},
		{spec.Feature{Type: "tinyint"}, "tinyint"},
		{spec.Feature{Type: "decimal(12,2)"}, "decimal(12,2)"},
		{spec.Feature{Type: "DOUBLE"}, "DOUBLE"},
		{spec.Feature{Type: "timestamp"}, "timestamp"},
		{spec.Feature{Type: "date"}, "date"},
		{spec.Feature{Type: "boolean"}, "tinyint"},
		{spec.Feature{Type: "string"}, "VARCHAR(100)"},
		{spec.Feature{Type: "array<string>"}, "VARBINARY(100)"},
		{spec.Feature{Type: "binary"}, "VARBINARY(100)"},
		{spec.Feature{Type: "timestamp", OnlineType: "TIMESTAMP(3)"}, "timestamp(3)"},
		{spec.Feature{Type: "string", OnlineType: "varchar(20)"}, "varchar(20)"},
	}
	for _, c := range cases {
		if got := OnlineType(c.f); got != c.want {
			t.Errorf("OnlineType(%+v) = %q, want %q", c.f, got, c.want)
		}
	}
}

func historyFG() spec.FeatureGroup {
	return spec.FeatureGroup{
		Name: "transactions", Version: 1, EventTime: "event_time",
		OnlineConfig: spec.OnlineConfig{SecondaryIndexes: [][]string{{"merchant_id"}}},
		Features: []spec.Feature{
			{Name: "customer_id", Type: "bigint", Primary: true},
			{Name: "event_time", Type: "timestamp", Primary: true},
			{Name: "amount", Type: "bigint"},
			{Name: "merchant_id", Type: "int"},
			{Name: "category", Type: "string"},
			{Name: "flag", Type: "boolean"},
		},
	}
}

func TestBuildCreateStatement_History(t *testing.T) {
	got, err := BuildCreateStatement("fs_test", historyFG())
	if err != nil {
		t.Fatal(err)
	}
	want := "CREATE TABLE IF NOT EXISTS `fs_test`.`transactions_1`(" +
		"`customer_id` bigint, `event_time` timestamp STORAGE MEMORY, `amount` bigint, " +
		"`merchant_id` int, `category` VARCHAR(100), `flag` tinyint, " +
		"PRIMARY KEY (`customer_id`,`event_time`), KEY `idx_merchant_id`(`merchant_id`))" +
		"ENGINE=ndbcluster COMMENT='NDB_TABLE=READ_BACKUP=1'"
	if got != want {
		t.Errorf("DDL mismatch\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildCreateStatement_HashPK_TTL_Tablespace_Default(t *testing.T) {
	ttl := int64(3153600000)
	def := "0"
	fg := spec.FeatureGroup{
		Name: "sessions", Version: 2, EventTime: "event_time", TTL: &ttl,
		OnlineConfig: spec.OnlineConfig{PrimaryKeyIndexType: "hash", TableSpace: "ts_1",
			SecondaryIndexes: [][]string{{"device", "pages"}}},
		Features: []spec.Feature{
			{Name: "customer_id", Type: "bigint", Primary: true},
			{Name: "event_time", Type: "timestamp", OnlineType: "timestamp(3)", Primary: true},
			{Name: "pages", Type: "int", DefaultValue: &def},
			{Name: "device", Type: "string"},
			{Name: "grain", Type: "string", OfflineOnly: true},
		},
	}
	got, err := BuildCreateStatement("db", fg)
	if err != nil {
		t.Fatal(err)
	}
	want := "CREATE TABLE IF NOT EXISTS `db`.`sessions_2`(" +
		"`customer_id` bigint, `event_time` timestamp(3) STORAGE MEMORY, " +
		"`pages` int NOT NULL DEFAULT 0, `device` VARCHAR(100), " +
		"PRIMARY KEY (`customer_id`,`event_time`) USING HASH, " +
		"KEY `ttl_index`(`event_time`), KEY `idx_device_pages`(`device`,`pages`))" +
		"ENGINE=ndbcluster COMMENT='NDB_TABLE=TTL=3153600000@event_time,READ_BACKUP=1'" +
		"/*!50100 TABLESPACE `ts_1` STORAGE DISK */"
	if got != want {
		t.Errorf("DDL mismatch\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildCreateStatement_StringDefaultAndExistingComments(t *testing.T) {
	def := "n/a"
	fg := spec.FeatureGroup{
		Name: "dim", Version: 1,
		OnlineConfig: spec.OnlineConfig{Comments: []string{"NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM"}},
		Features: []spec.Feature{
			{Name: "id", Type: "int", Primary: true},
			{Name: "label", Type: "string", DefaultValue: &def},
		},
	}
	got, err := BuildCreateStatement("db", fg)
	if err != nil {
		t.Fatal(err)
	}
	want := "CREATE TABLE IF NOT EXISTS `db`.`dim_1`(`id` int, `label` VARCHAR(100) NOT NULL DEFAULT 'n/a', " +
		"PRIMARY KEY (`id`))ENGINE=ndbcluster COMMENT='NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM,READ_BACKUP=1'"
	if got != want {
		t.Errorf("DDL mismatch\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildCreateStatement_Errors(t *testing.T) {
	fg := historyFG()
	fg.OnlineConfig.PrimaryKeyIndexType = "BTREE"
	if _, err := BuildCreateStatement("db", fg); err == nil {
		t.Error("expected error for invalid primaryKeyIndexType")
	}
	fg = historyFG()
	fg.OnlineConfig.SecondaryIndexes = [][]string{{"no_such_column"}}
	if _, err := BuildCreateStatement("db", fg); err == nil {
		t.Error("expected error for unknown secondary index column")
	}
	fg = historyFG()
	fg.Features[1].OfflineOnly = true
	if _, err := BuildCreateStatement("db", fg); err == nil {
		t.Error("expected error for offline_only primary key column")
	}
}

func TestComments(t *testing.T) {
	fg := spec.FeatureGroup{OnlineConfig: spec.OnlineConfig{Comments: []string{"NDB_TABLE=READ_BACKUP=0"}}}
	if got := Comments(fg); len(got) != 1 || got[0] != "NDB_TABLE=READ_BACKUP=0" {
		t.Errorf("READ_BACKUP already present must be kept as is, got %v", got)
	}
}
