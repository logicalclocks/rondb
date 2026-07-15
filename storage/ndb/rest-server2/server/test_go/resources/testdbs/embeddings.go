/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023 Hopsworks AB
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

package testdbs

import _ "embed"

/*
	Dynamic schemes
*/

//go:embed dynamic/benchmark.sql
var BenchmarkScheme string

const Benchmark = "rdrs_bench"
const benchmarkSed_COLUMN_LENGTH = "COLUMN_LENGTH"
const benchmarkSed_VARBINARY_PK_LENGTH_LENGTH = "VARBINARY_PK_LENGTH"
const benchmarkSed_MANY_IDENTICAL_COLUMNS = "MANY_IDENTICAL_COLUMNS"

//go:embed dynamic/benchmark_add_row.sql
var BenchmarkAddRow string

// Seding values
const BenchAddRow_TABLE_NAME = "TABLE_NAME"
const BenchAddRow_COLUMN_VALUES_TO_INSERT = "COLUMN_VALUES_TO_INSERT"

// Curated hopsworks fixture data, dumped at post-DDL schema level.
// Loaded AFTER the hopsworks-ddl migration patches (see HopsworksScheme).
//go:embed fixed/hopsworks-data/hopsworks_data.sql
var HopsworksData string

//go:embed fixed/hopsworks_40_schema.sql
var HopsworksSchema string

//Upgrades / patches
//V5-FSTORE-1537-managed_feature_group.sql
//V6-FSTORE-1507-python_udfs.sql
//V7-HWORKS-1627-kube_labels_priorityclasses.sql
//V8-HWORKS-1670-ray_integration.sql
//V9-FSTORE-1592-type_column_size.sql
//V10-FSTORE-1598-FSTORE-1595-avro_schema_fixes.sql

//go:embed fixed/hopsworks-ddl/V5-FSTORE-1537-managed_feature_group.sql
var V5 string

//go:embed fixed/hopsworks-ddl/V6-FSTORE-1507-python_udfs.sql
var V6 string

//go:embed fixed/hopsworks-ddl/V7-HWORKS-1627-kube_labels_priorityclasses.sql
var V7 string

//go:embed fixed/hopsworks-ddl/V8-HWORKS-1670-ray_integration.sql
var V8 string

//go:embed fixed/hopsworks-ddl/V9-FSTORE-1592-type_column_size.sql
var V9 string

//go:embed fixed/hopsworks-ddl/V10-FSTORE-1598-FSTORE-1595-avro_schema_fixes.sql
var V10 string

//go:embed fixed/hopsworks-ddl/V11-FSTORE-1581-fix_deletion_of_feature_group_transformation_functions.sql
var V11 string

//go:embed fixed/hopsworks-ddl/V12-HWORKS-1862-fix_subject_deletion.sql
var V12 string

//go:embed fixed/hopsworks-ddl/V13-FSTORE-1436-foreign_key.sql
var V13 string

//go:embed fixed/hopsworks-ddl/V14-FSTORE-1642-adding_user_scope_to_serving_api_key.sql
var V14 string

//go:embed fixed/hopsworks-ddl/V15-FSTORE-1605-adding_path_field_to_s3_storage_connector.sql
var V15 string

//go:embed fixed/hopsworks-ddl/V16-HWORKS-1885-add_vllm_openai_image.sql
var V16 string

//go:embed fixed/hopsworks-ddl/V17-FSTORE-1630-output_column_names_transformation_functions.sql
var V17 string

//go:embed fixed/hopsworks-ddl/V18-HWORKS-1941-replicate_kube_ops.sql
var V18 string

//go:embed fixed/hopsworks-ddl/V19-FSTORE-1580-onlinefs_observability.sql
var V19 string

//go:embed fixed/hopsworks-ddl/V20-FSTORE-1672-increase_size_for_output_type_column_transformation_functions.sql
var V20 string

//go:embed fixed/hopsworks-ddl/V21-FSTORE-1668-alert_for_jobs_that_are_stuck.sql
var V21 string

//go:embed fixed/hopsworks-ddl/V22-FSTORE-1686-set_default_execution_mode.sql
var V22 string

//go:embed fixed/hopsworks-ddl/V23-HWORKS-2076-Increase_schema_field_size_to_mediumtext.sql
var V23 string

//go:embed fixed/hopsworks-ddl/V24-HWORKS-1894-banner.sql
var V24 string

//go:embed fixed/hopsworks-ddl/V25-FSTORE-1692-add_lastvisitedat_to_userprofile.sql
var V25 string

//go:embed fixed/hopsworks-ddl/V26-FSTORE-1651-dynamic_query_online_fs.sql
var V26 string

//go:embed fixed/hopsworks-ddl/V27-FSTORE-1714-increase_feature_name_size_feature_descriptive_statistics_to_allow_fully_qualified_names.sql
var V27 string

//go:embed fixed/hopsworks-ddl/V28-FSTORE-1698-data_sources.sql
var V28 string

//go:embed fixed/hopsworks-ddl/V29-LA-101-brewer.sql
var V29 string

//go:embed fixed/hopsworks-ddl/V30-FSTORE-1751-increase_output_features_size_on_demand_transformations.sql
var V30 string

//go:embed fixed/hopsworks-ddl/V31-HWORKS-2145-external_access_to_model_deployments.sql
var V31 string

//go:embed fixed/hopsworks-ddl/V32-FSTORE-1745-add_s3_ro_iam_role.sql
var V32 string

//go:embed fixed/hopsworks-ddl/V33-FSTORE-1736-Managed_Feature_Groups_on_S3_are_not_properly_index_for_search.sql
var V33 string

//go:embed fixed/hopsworks-ddl/V34-HWORKS-2183-feature_store_metrics.sql
var V34 string

//go:embed fixed/hopsworks-ddl/V35-FSTORE-1731-add_ttl_to_feature_group.sql
var V35 string

//go:embed fixed/hopsworks-ddl/V36-HWORKS-1912-group_to_project_mapping.sql
var V36 string

//go:embed fixed/hopsworks-ddl/V37-FSTORE-1834-user_generated_charts.sql
var V37 string

//go:embed fixed/hopsworks-ddl/V38-HWORKS-103-split_serving_entity.sql
var V38 string

//go:embed fixed/hopsworks-ddl/V39-HWORKS-1186-modelless_deployments.sql
var V39 string

//go:embed fixed/hopsworks-ddl/V40-FSTORE-1871-adding_type_to_serving_keys.sql
var V40 string

//go:embed fixed/hopsworks-ddl/V41-HWORKS-2406-git_commit_message_type.sql
var V41 string

//go:embed fixed/hopsworks-ddl/V42-FSTORE-1918-Change_feature_group_commit_data_types_to_bigint.sql
var V42 string

//go:embed fixed/hopsworks-ddl/V43-FSTORE-1438-add_key_path_snowflake_connector.sql
var V43 string

//go:embed fixed/hopsworks-ddl/V44-FSTORE-1901-opensearch_storage_connector.sql
var V44 string

//go:embed fixed/hopsworks-ddl/V45-FSTORE-1905-column_level_permissions.sql
var V45 string

//go:embed fixed/hopsworks-ddl/V46-FSTORE-1940-restricted_feature_access.sql
var V46 string

//go:embed fixed/hopsworks-ddl/V47-FSTORE-1945-add_system_theme.sql
var V47 string

//go:embed fixed/hopsworks-ddl/V48-HWORKS-2502-kserve_autoscaling_configs.sql
var V48 string

//go:embed fixed/hopsworks-ddl/V49-HWORKS-2233-mandatory_tags.sql
var V49 string

//go:embed fixed/hopsworks-ddl/V50-BREWER-152-agent_garden.sql
var V50 string

//go:embed fixed/hopsworks-ddl/V51-FSTORE-1946-share_datasets.sql
var V51 string

//go:embed fixed/hopsworks-ddl/V52-HWORKS-2415-operation_logs.sql
var V52 string

//go:embed fixed/hopsworks-ddl/V53-HWORKS-2558-feature_group_feature_usage.sql
var V53 string

//go:embed fixed/hopsworks-ddl/V54-HWORKS-2391-dlthub.sql
var V54 string

//go:embed fixed/hopsworks-ddl/V55-FSTORE-1795-data_source_api_updates.sql
var V55 string

//go:embed fixed/hopsworks-ddl/V56-HWORKS-2606-shell_terminal.sql
var V56 string

//go:embed fixed/hopsworks-ddl/V57-FSTORE-1795-rename_rds_to_sql_connector.sql
var V57 string

//go:embed fixed/hopsworks-ddl/V58-HWORKS-2665-deprecated_environment.sql
var V58 string

//go:embed fixed/hopsworks-ddl/V59-HWORKS-2525-trino_queries.sql
var V59 string

//go:embed fixed/hopsworks-ddl/V60-HWORKS-2606-terminal_spark_and_coding_agent.sql
var V60 string

//go:embed fixed/hopsworks-ddl/V61-FSTORE-2011-merge_oracle_into_sql_connector.sql
var V61 string

//go:embed fixed/hopsworks-ddl/V62-FSTORE-1967-logical_time_scheduling.sql
var V62 string

//go:embed fixed/hopsworks-ddl/V63-FSTORE-2017-unity_catalog_connector.sql
var V63 string

//go:embed fixed/hopsworks-ddl/V64-FSTORE-1967-deployment_env_vars.sql
var V64 string

//go:embed fixed/hopsworks-ddl/V65-FSTORE-2050-sap_hana_storage_connector.sql
var V65 string

//go:embed fixed/hopsworks-ddl/V66-HWORKS-2710-user_env_vars.sql
var V66 string

//go:embed fixed/hopsworks-ddl/V67-HWORKS-2755-vllm_variant_version.sql
var V67 string

//go:embed fixed/hopsworks-ddl/V68-HWORKS-2667-pass_to_agent_job_alert.sql
var V68 string

//go:embed fixed/hopsworks-ddl/V69-FSTORE-2028-mongodb_storage_connector.sql
var V69 string

//go:embed fixed/hopsworks-ddl/V70-FSTORE-2036-uc_oauth_m2m.sql
var V70 string

//go:embed fixed/hopsworks-ddl/V71-HWORKS-2810-max_queued_executions_per_user_per_job.sql
var V71 string

//go:embed fixed/hopsworks-ddl/V72-HWORKS-2815-user_env_vars_secret_name.sql
var V72 string

//go:embed fixed/hopsworks-ddl/V73-HWORKS-2804-add_expiry_to_api_key.sql
var V73 string

//go:embed fixed/hopsworks-ddl/V74-FSTORE-2030-pit_join_lookback_window.sql
var V74 string

//go:embed fixed/hopsworks-ddl/V75-FSTORE-2024-default_featurestore_project_name.sql
var V75 string

//go:embed fixed/hopsworks-ddl/V76-HWORKS-2816-serving_tracing_config.sql
var V76 string

//go:embed fixed/hopsworks-ddl/V77-HWORKS-2869-python_app_proxy_path_mode.sql
var V77 string

//go:embed fixed/hopsworks-ddl/V78-FSTORE-1412-feature_monitoring_v2.sql
var V78 string

//go:embed fixed/hopsworks-ddl/V79-HWORKS-2802-partitioned_by.sql
var V79 string

//go:embed fixed/hopsworks-ddl/V80-HWORKS-2871-agent_deployments_git_source.sql
var V80 string

//go:embed fixed/hopsworks-ddl/V81-FSTORE-2047-glue_storage_connector.sql
var V81 string

//go:embed fixed/hopsworks-ddl/V82-HWORKS-2789-remove_brewer.sql
var V82 string

//go:embed fixed/hopsworks-ddl/V83-HWORKS-2878-google_sheets_connector.sql
var V83 string

var HopsworksScheme string = HopsworksSchema +
	V5 + V6 + V7 + V8 + V9 + V10 +
	V11 + V12 + V13 + V14 + V15 + V16 + V17 + V18 + V19 + V20 +
	V21 + V22 + V23 + V24 + V25 + V26 + V27 + V28 + V29 + V30 +
	V31 + V32 + V33 + V34 + V35 + V36 + V37 + V38 + V39 + V40 +
	V41 + V42 + V43 + V44 + V45 + V46 + V47 + V48 + V49 + V50 +
	V51 + V52 + V53 + V54 + V55 + V56 + V57 + V58 + V59 + V60 +
	V61 + V62 + V63 + V64 + V65 + V66 + V67 + V68 + V69 + V70 +
	V71 + V72 + V73 + V74 + V75 + V76 + V77 + V78 + V79 + V80 +
	V81 + V82 + V83 +
	HopsworksData

const HOPSWORKS_DB_NAME = "hopsworks"

//go:embed dynamic/hopsworks_add_project.sql
var HopsworksAddProject string

//go:embed fixed/hopsworks_fs_update.sql
var HopsworksUpdateScheme string

const hopsworksAddProject_PROJECT_NAME = "PROJECT_NAME"
const hopsworksAddProject_PROJECT_NUMBER = "PROJECT_NUMBER"

//go:embed dynamic/hopsworks_api_key.sql
var HopsworksAPIKey string

const HopsworksAPIKey_KEY_ID = "KEY_ID"
const HopsworksAPIKey_KEY_PREFIX = "KEY_PREFIX"
const HopsworksAPIKey_KEY_NAME = "KEY_NAME"
const HopsworksAPIKey_ADDITIONAL_KEYS = 512
const HopsworksAPIKey_SECRET = "ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub"

//go:embed dynamic/textual_columns.sql
var TextualColumns string

const textualColumns_DATABASE_NAME = "DATABASE_NAME"
const textualColumns_COLUMN_TYPE = "COLUMN_TYPE"
const textualColumns_COLUMN_LENGTH = "COLUMN_LENGTH"

const DB012 = "db012"
const DB014 = "db014"
const DB015 = "db015"
const DB016 = "db016"
const DB017 = "db017"
const DB018 = "db018"

/*
	Fixed schemes

	TODO:

	Add the following constants for all tables to avoid dynamic typing of the tables all over the code:
	const DB001_table1 = "table_1"
	const DB002_table1 = "table_1"
*/

//go:embed fixed/DB000.sql
var DB000Scheme string

const DB000 = "db000"

//go:embed fixed/DB001.sql
var DB001Scheme string

const DB001 = "db001"

//go:embed fixed/DB002.sql
var DB002Scheme string

const DB002 = "db002"

//go:embed fixed/DB003.sql
var DB003Scheme string

const DB003 = "db003"

//go:embed fixed/DB004.sql
var DB004Scheme string

const DB004 = "db004"

//go:embed fixed/DB005.sql
var DB005Scheme string

const DB005 = "db005"

//go:embed fixed/DB006.sql
var DB006Scheme string

const DB006 = "db006"

//go:embed fixed/DB007.sql
var DB007Scheme string

const DB007 = "db007"

//go:embed fixed/DB008.sql
var DB008Scheme string

const DB008 = "db008"

//go:embed fixed/DB009.sql
var DB009Scheme string

const DB009 = "db009"

//go:embed fixed/DB010.sql
var DB010Scheme string

const DB010 = "db010"

//go:embed fixed/DB011.sql
var DB011Scheme string

const DB011 = "db011"

//go:embed fixed/DB013.sql
var DB013Scheme string

const DB013 = "db013"

//go:embed fixed/DB019.sql
var DB019Scheme string

const DB019 = "db019"

//go:embed fixed/DB020.sql
var DB020Scheme string

const DB020 = "db020"

//go:embed fixed/DB021.sql
var DB021Scheme string

const DB021 = "db021"

//go:embed fixed/DB022.sql
var DB022Scheme string

const DB022 = "db022"

//go:embed fixed/DB023.sql
var DB023Scheme string

const DB023 = "db023"

//go:embed fixed/DB024.sql
var DB024Scheme string

const DB024 = "db024"

//go:embed fixed/DB025.sql
var DB025Scheme string

const DB025 = "db025"

//go:embed fixed/DB025-Update.sql
var DB025UpdateScheme string

//go:embed fixed/DB025-Perm.sql
var DB025PermScheme string

//go:embed fixed/DB025-Perm-Update.sql
var DB025PermUpdateScheme string

//go:embed fixed/DB026.sql
var DB026Scheme string

const DB026 = "db026"

//go:embed fixed/DB027.sql
var DB027Scheme string

const DB027 = "db027"

//go:embed fixed/DB028.sql
var DB028Scheme string

const DB028 = "db028"

//go:embed fixed/DB029.sql
var DB029Scheme string

const DB029 = "db029"

//go:embed fixed/DB030.sql
var DB030Scheme string

const DB030 = "db030"

//go:embed fixed/FSDB001.sql
var FSDB001Scheme string

const FSDB001 = "fsdb001"

//go:embed fixed/FSDB002.sql
var FSDB002Scheme string

const FSDB002 = "fsdb002"

//go:embed fixed/FSDB003.sql
var FSDB003Scheme string

const FSDB003 = "fsdb003"

//go:embed fixed/FSDB004.sql
var FSDB004Scheme string

const FSDB004 = "fsdb004"

// This is sentinel DB
// If this exists then we have successfully initialized all the DBs
//
//go:embed fixed/sentinel.sql
var SentinelDBScheme string

const SentinelDB = "sentinel"
