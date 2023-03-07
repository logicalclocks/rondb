package testdbs

import _ "embed"

/*
	Dynamic schemes
*/

//go:embed dynamic/benchmark.sql
var BenchmarkScheme string

const Benchmark = "RDRS_BENCH"
const benchmarkSed_COLUMN_LENGTH = "COLUMN_LENGTH"

//go:embed dynamic/benchmark_add_row.sql
var BenchmarkAddRow string

const BenchmarkAddRowSed_VALUE_COLUMN_1 = "VALUE_COLUMN_1"
const BenchmarkAddRowSed_VALUE_COLUMN_2 = "VALUE_COLUMN_2"

//go:embed dynamic/hopsworks_add_project.sql
var HopsworksAddProject string

const hopsworksAddProject_PROJECT_NAME = "PROJECT_NAME"
const hopsworksAddProject_PROJECT_NUMBER = "PROJECT_NUMBER"

//go:embed dynamic/textual_columns.sql
var TextualColumns string

const textualColumns_DATABASE_NAME = "DATABASE_NAME"
const textualColumns_COLUMN_TYPE = "COLUMN_TYPE"
const textualColumns_COLUMN_LENGTH = "COLUMN_LENGTH"

const DB012 = "DB012"
const DB014 = "DB014"
const DB015 = "DB015"
const DB016 = "DB016"
const DB017 = "DB017"
const DB018 = "DB018"

/*
	Fixed schemes

	TODO:

	Add the following constants for all tables to avoid dynamic typing of the tables all over the code:
	const DB001_table1 = "table_1"
	const DB002_table1 = "table_1"
*/

//go:embed fixed/hopsworks.sql
var HopsworksScheme string

const HOPSWORKS_DB_NAME = "hopsworks"

//go:embed fixed/DB000.sql
var DB000Scheme string

const DB000 = "DB000"

//go:embed fixed/DB001.sql
var DB001Scheme string

const DB001 = "DB001"

//go:embed fixed/DB002.sql
var DB002Scheme string

const DB002 = "DB002"

//go:embed fixed/DB003.sql
var DB003Scheme string

const DB003 = "DB003"

//go:embed fixed/DB004.sql
var DB004Scheme string

const DB004 = "DB004"

//go:embed fixed/DB005.sql
var DB005Scheme string

const DB005 = "DB005"

//go:embed fixed/DB006.sql
var DB006Scheme string

const DB006 = "DB006"

//go:embed fixed/DB007.sql
var DB007Scheme string

const DB007 = "DB007"

//go:embed fixed/DB008.sql
var DB008Scheme string

const DB008 = "DB008"

//go:embed fixed/DB009.sql
var DB009Scheme string

const DB009 = "DB009"

//go:embed fixed/DB010.sql
var DB010Scheme string

const DB010 = "DB010"

//go:embed fixed/DB011.sql
var DB011Scheme string

const DB011 = "DB011"

//go:embed fixed/DB013.sql
var DB013Scheme string

const DB013 = "DB013"

//go:embed fixed/DB019.sql
var DB019Scheme string

const DB019 = "DB019"

//go:embed fixed/DB020.sql
var DB020Scheme string

const DB020 = "DB020"

//go:embed fixed/DB021.sql
var DB021Scheme string

const DB021 = "DB021"

//go:embed fixed/DB022.sql
var DB022Scheme string

const DB022 = "DB022"

//go:embed fixed/DB023.sql
var DB023Scheme string

const DB023 = "DB023"

//go:embed fixed/DB024.sql
var DB024Scheme string

const DB024 = "DB024"
