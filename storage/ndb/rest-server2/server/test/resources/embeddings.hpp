/*
 * Copyright (C) 2024 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_TEST_RESOURCES_EMBEDDINGS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_TEST_RESOURCES_EMBEDDINGS_HPP_

#include "embedded_content.hpp"

#include <string>

// Constants
const std::string Benchmark = "rdrs_bench";
const std::string benchmarkSed_COLUMN_LENGTH = "COLUMN_LENGTH";

const std::string benchmarkSed_VARBINARY_PK_LENGTH_LENGTH = "VARBINARY_PK_LENGTH";
const std::string benchmarkSed_MANY_IDENTICAL_COLUMNS = "MANY_IDENTICAL_COLUMNS";

//go:embed dynamic/benchmark_add_row.sql
const std::string BenchmarkAddRow = sqlFiles.at("dynamic/benchmark_add_row.sql");

// Seding values
const std::string BenchAddRow_TABLE_NAME = "TABLE_NAME";
const std::string BenchAddRow_COLUMN_VALUES_TO_INSERT = "COLUMN_VALUES_TO_INSERT";

//go:embed fixed/hopsworks_40_data.sql
const std::string HopsworksData = sqlFiles.at("fixed/hopsworks_40_data.sql");

//go:embed fixed/hopsworks_40_schema.sql
const std::string HopsworksSchema = sqlFiles.at("fixed/hopsworks_40_schema.sql");

// Hopsworks schema migration patches (kept in sync with test_go/resources/testdbs)
const std::string V5 = sqlFiles.at("fixed/hopsworks-ddl/V5-FSTORE-1537-managed_feature_group.sql");
const std::string V6 = sqlFiles.at("fixed/hopsworks-ddl/V6-FSTORE-1507-python_udfs.sql");
const std::string V7 = sqlFiles.at("fixed/hopsworks-ddl/V7-HWORKS-1627-kube_labels_priorityclasses.sql");
const std::string V8 = sqlFiles.at("fixed/hopsworks-ddl/V8-HWORKS-1670-ray_integration.sql");
const std::string V9 = sqlFiles.at("fixed/hopsworks-ddl/V9-FSTORE-1592-type_column_size.sql");
const std::string V10 = sqlFiles.at("fixed/hopsworks-ddl/V10-FSTORE-1598-FSTORE-1595-avro_schema_fixes.sql");
const std::string V11 = sqlFiles.at("fixed/hopsworks-ddl/V11-FSTORE-1581-fix_deletion_of_feature_group_transformation_functions.sql");
const std::string V12 = sqlFiles.at("fixed/hopsworks-ddl/V12-HWORKS-1862-fix_subject_deletion.sql");
const std::string V13 = sqlFiles.at("fixed/hopsworks-ddl/V13-FSTORE-1436-foreign_key.sql");
const std::string V14 = sqlFiles.at("fixed/hopsworks-ddl/V14-FSTORE-1642-adding_user_scope_to_serving_api_key.sql");
const std::string V15 = sqlFiles.at("fixed/hopsworks-ddl/V15-FSTORE-1605-adding_path_field_to_s3_storage_connector.sql");
const std::string V16 = sqlFiles.at("fixed/hopsworks-ddl/V16-HWORKS-1885-add_vllm_openai_image.sql");
const std::string V17 = sqlFiles.at("fixed/hopsworks-ddl/V17-FSTORE-1630-output_column_names_transformation_functions.sql");
const std::string V18 = sqlFiles.at("fixed/hopsworks-ddl/V18-HWORKS-1941-replicate_kube_ops.sql");
const std::string V19 = sqlFiles.at("fixed/hopsworks-ddl/V19-FSTORE-1580-onlinefs_observability.sql");
const std::string V20 = sqlFiles.at("fixed/hopsworks-ddl/V20-FSTORE-1672-increase_size_for_output_type_column_transformation_functions.sql");
const std::string V21 = sqlFiles.at("fixed/hopsworks-ddl/V21-FSTORE-1668-alert_for_jobs_that_are_stuck.sql");
const std::string V22 = sqlFiles.at("fixed/hopsworks-ddl/V22-FSTORE-1686-set_default_execution_mode.sql");
const std::string V23 = sqlFiles.at("fixed/hopsworks-ddl/V23-HWORKS-2076-Increase_schema_field_size_to_mediumtext.sql");
const std::string V24 = sqlFiles.at("fixed/hopsworks-ddl/V24-HWORKS-1894-banner.sql");
const std::string V25 = sqlFiles.at("fixed/hopsworks-ddl/V25-FSTORE-1692-add_lastvisitedat_to_userprofile.sql");
const std::string V26 = sqlFiles.at("fixed/hopsworks-ddl/V26-FSTORE-1651-dynamic_query_online_fs.sql");
const std::string V27 = sqlFiles.at("fixed/hopsworks-ddl/V27-FSTORE-1714-increase_feature_name_size_feature_descriptive_statistics_to_allow_fully_qualified_names.sql");
const std::string V28 = sqlFiles.at("fixed/hopsworks-ddl/V28-FSTORE-1698-data_sources.sql");
const std::string V29 = sqlFiles.at("fixed/hopsworks-ddl/V29-LA-101-brewer.sql");
const std::string V30 = sqlFiles.at("fixed/hopsworks-ddl/V30-FSTORE-1751-increase_output_features_size_on_demand_transformations.sql");
const std::string V31 = sqlFiles.at("fixed/hopsworks-ddl/V31-HWORKS-2145-external_access_to_model_deployments.sql");
const std::string V32 = sqlFiles.at("fixed/hopsworks-ddl/V32-FSTORE-1745-add_s3_ro_iam_role.sql");
const std::string V33 = sqlFiles.at("fixed/hopsworks-ddl/V33-FSTORE-1736-Managed_Feature_Groups_on_S3_are_not_properly_index_for_search.sql");
const std::string V34 = sqlFiles.at("fixed/hopsworks-ddl/V34-HWORKS-2183-feature_store_metrics.sql");
const std::string V35 = sqlFiles.at("fixed/hopsworks-ddl/V35-FSTORE-1731-add_ttl_to_feature_group.sql");
const std::string V36 = sqlFiles.at("fixed/hopsworks-ddl/V36-HWORKS-1912-group_to_project_mapping.sql");
const std::string V37 = sqlFiles.at("fixed/hopsworks-ddl/V37-FSTORE-1834-user_generated_charts.sql");
const std::string V38 = sqlFiles.at("fixed/hopsworks-ddl/V38-HWORKS-103-split_serving_entity.sql");
const std::string V39 = sqlFiles.at("fixed/hopsworks-ddl/V39-HWORKS-1186-modelless_deployments.sql");
const std::string V40 = sqlFiles.at("fixed/hopsworks-ddl/V40-FSTORE-1871-adding_type_to_serving_keys.sql");
const std::string V41 = sqlFiles.at("fixed/hopsworks-ddl/V41-HWORKS-2406-git_commit_message_type.sql");
const std::string V42 = sqlFiles.at("fixed/hopsworks-ddl/V42-FSTORE-1918-Change_feature_group_commit_data_types_to_bigint.sql");
const std::string V43 = sqlFiles.at("fixed/hopsworks-ddl/V43-FSTORE-1438-add_key_path_snowflake_connector.sql");
const std::string V44 = sqlFiles.at("fixed/hopsworks-ddl/V44-FSTORE-1901-opensearch_storage_connector.sql");
const std::string V45 = sqlFiles.at("fixed/hopsworks-ddl/V45-FSTORE-1905-column_level_permissions.sql");
const std::string V46 = sqlFiles.at("fixed/hopsworks-ddl/V46-FSTORE-1940-restricted_feature_access.sql");
const std::string V47 = sqlFiles.at("fixed/hopsworks-ddl/V47-FSTORE-1945-add_system_theme.sql");
const std::string V48 = sqlFiles.at("fixed/hopsworks-ddl/V48-HWORKS-2502-kserve_autoscaling_configs.sql");
const std::string V49 = sqlFiles.at("fixed/hopsworks-ddl/V49-HWORKS-2233-mandatory_tags.sql");
const std::string V50 = sqlFiles.at("fixed/hopsworks-ddl/V50-BREWER-152-agent_garden.sql");
const std::string V51 = sqlFiles.at("fixed/hopsworks-ddl/V51-FSTORE-1946-share_datasets.sql");
const std::string V52 = sqlFiles.at("fixed/hopsworks-ddl/V52-HWORKS-2415-operation_logs.sql");
const std::string V53 = sqlFiles.at("fixed/hopsworks-ddl/V53-HWORKS-2558-feature_group_feature_usage.sql");
const std::string V54 = sqlFiles.at("fixed/hopsworks-ddl/V54-HWORKS-2391-dlthub.sql");
const std::string V55 = sqlFiles.at("fixed/hopsworks-ddl/V55-FSTORE-1795-data_source_api_updates.sql");
const std::string V56 = sqlFiles.at("fixed/hopsworks-ddl/V56-HWORKS-2606-shell_terminal.sql");
const std::string V57 = sqlFiles.at("fixed/hopsworks-ddl/V57-FSTORE-1795-rename_rds_to_sql_connector.sql");
const std::string V58 = sqlFiles.at("fixed/hopsworks-ddl/V58-HWORKS-2665-deprecated_environment.sql");
const std::string V59 = sqlFiles.at("fixed/hopsworks-ddl/V59-HWORKS-2525-trino_queries.sql");
const std::string V60 = sqlFiles.at("fixed/hopsworks-ddl/V60-HWORKS-2606-terminal_spark_and_coding_agent.sql");
const std::string V61 = sqlFiles.at("fixed/hopsworks-ddl/V61-FSTORE-2011-merge_oracle_into_sql_connector.sql");
const std::string V62 = sqlFiles.at("fixed/hopsworks-ddl/V62-FSTORE-1967-logical_time_scheduling.sql");
const std::string V63 = sqlFiles.at("fixed/hopsworks-ddl/V63-FSTORE-2017-unity_catalog_connector.sql");
const std::string V64 = sqlFiles.at("fixed/hopsworks-ddl/V64-FSTORE-1967-deployment_env_vars.sql");
const std::string V65 = sqlFiles.at("fixed/hopsworks-ddl/V65-FSTORE-2050-sap_hana_storage_connector.sql");
const std::string V66 = sqlFiles.at("fixed/hopsworks-ddl/V66-HWORKS-2710-user_env_vars.sql");
const std::string V67 = sqlFiles.at("fixed/hopsworks-ddl/V67-HWORKS-2755-vllm_variant_version.sql");
const std::string V68 = sqlFiles.at("fixed/hopsworks-ddl/V68-HWORKS-2667-pass_to_agent_job_alert.sql");
const std::string V69 = sqlFiles.at("fixed/hopsworks-ddl/V69-FSTORE-2028-mongodb_storage_connector.sql");
const std::string V70 = sqlFiles.at("fixed/hopsworks-ddl/V70-FSTORE-2036-uc_oauth_m2m.sql");
const std::string V71 = sqlFiles.at("fixed/hopsworks-ddl/V71-HWORKS-2810-max_queued_executions_per_user_per_job.sql");
const std::string V72 = sqlFiles.at("fixed/hopsworks-ddl/V72-HWORKS-2815-user_env_vars_secret_name.sql");
const std::string V73 = sqlFiles.at("fixed/hopsworks-ddl/V73-HWORKS-2804-add_expiry_to_api_key.sql");
const std::string V74 = sqlFiles.at("fixed/hopsworks-ddl/V74-FSTORE-2030-pit_join_lookback_window.sql");
const std::string V75 = sqlFiles.at("fixed/hopsworks-ddl/V75-FSTORE-2024-default_featurestore_project_name.sql");
const std::string V76 = sqlFiles.at("fixed/hopsworks-ddl/V76-HWORKS-2816-serving_tracing_config.sql");
const std::string V77 = sqlFiles.at("fixed/hopsworks-ddl/V77-HWORKS-2869-python_app_proxy_path_mode.sql");
const std::string V78 = sqlFiles.at("fixed/hopsworks-ddl/V78-FSTORE-1412-feature_monitoring_v2.sql");
const std::string V79 = sqlFiles.at("fixed/hopsworks-ddl/V79-HWORKS-2802-partitioned_by.sql");
const std::string V80 = sqlFiles.at("fixed/hopsworks-ddl/V80-HWORKS-2871-agent_deployments_git_source.sql");
const std::string V81 = sqlFiles.at("fixed/hopsworks-ddl/V81-FSTORE-2047-glue_storage_connector.sql");
const std::string V82 = sqlFiles.at("fixed/hopsworks-ddl/V82-HWORKS-2789-remove_brewer.sql");
const std::string V83 = sqlFiles.at("fixed/hopsworks-ddl/V83-HWORKS-2878-google_sheets_connector.sql");

const std::string HopsworksScheme = HopsworksSchema + HopsworksData +
    V5 + V6 + V7 + V8 + V9 + V10 + V11 + V12 + V13 + V14 +
    V15 + V16 + V17 + V18 + V19 + V20 + V21 + V22 + V23 + V24 +
    V25 + V26 + V27 + V28 + V29 + V30 + V31 + V32 + V33 + V34 +
    V35 + V36 + V37 + V38 + V39 + V40 + V41 + V42 + V43 + V44 +
    V45 + V46 + V47 + V48 + V49 + V50 + V51 + V52 + V53 + V54 +
    V55 + V56 + V57 + V58 + V59 + V60 + V61 + V62 + V63 + V64 +
    V65 + V66 + V67 + V68 + V69 + V70 + V71 + V72 + V73 + V74 +
    V75 + V76 + V77 + V78 + V79 + V80 + V81 + V82 + V83;

const std::string HOPSWORKS_DB_NAME = "hopsworks";

//go:embed dynamic/hopsworks_34_add_project.sql
const std::string HopsworksAddProject = sqlFiles.at("dynamic/hopsworks_34_add_project.sql");

//go:embed fixed/hopsworks_fs_update.sql
const std::string HopsworksUpdateScheme = sqlFiles.at("fixed/hopsworks_fs_update.sql");

const std::string hopsworksAddProject_PROJECT_NAME = "PROJECT_NAME";
const std::string hopsworksAddProject_PROJECT_NUMBER = "PROJECT_NUMBER";

//go:embed dynamic/hopsworks_api_key.sql
const std::string HopsworksAPI_Key = sqlFiles.at("dynamic/hopsworks_api_key.sql");

const std::string HopsworksAPIKey_KEY_ID = "KEY_ID";
const std::string HopsworksAPIKey_KEY_PREFIX = "KEY_PREFIX";
const std::string HopsworksAPIKey_KEY_NAME = "KEY_NAME";
const int HopsworksAPIKey_ADDITIONAL_KEYS = 512;
const std::string HopsworksAPIKey_SECRET = "ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub";

//go:embed dynamic/textual_columns.sql
const std::string TextualColumns = sqlFiles.at("dynamic/textual_columns.sql");

const std::string textualColumns_DATABASE_NAME = "DATABASE_NAME";
const std::string textualColumns_COLUMN_TYPE = "COLUMN_TYPE";
const std::string textualColumns_COLUMN_LENGTH = "COLUMN_LENGTH";

const std::string DB012 = "db012";
const std::string DB014 = "db014";
const std::string DB015 = "db015";
const std::string DB016 = "db016";
const std::string DB017 = "db017";
const std::string DB018 = "db018";

/*
	Fixed schemes

	TODO:

	Add the following constants for all tables to avoid dynamic typing of the tables all over the code:
	const DB001_table1 = "table_1"
	const DB002_table1 = "table_1"
*/

//go:embed fixed/DB000.sql
const std::string DB000Scheme = sqlFiles.at("fixed/DB000.sql");

const std::string DB000 = "db000";

//go:embed fixed/DB001.sql
const std::string DB001Scheme = sqlFiles.at("fixed/DB001.sql");

const std::string DB001 = "db001";

//go:embed fixed/DB002.sql
const std::string DB002Scheme = sqlFiles.at("fixed/DB002.sql");

const std::string DB002 = "db002";

//go:embed fixed/DB003.sql
const std::string DB003Scheme = sqlFiles.at("fixed/DB003.sql");

const std::string DB003 = "db003";

//go:embed fixed/DB004.sql
const std::string DB004Scheme = sqlFiles.at("fixed/DB004.sql");

const std::string DB004 = "db004";

//go:embed fixed/DB005.sql
const std::string DB005Scheme = sqlFiles.at("fixed/DB005.sql");

const std::string DB005 = "db005";

//go:embed fixed/DB006.sql
const std::string DB006Scheme = sqlFiles.at("fixed/DB006.sql");

const std::string DB006 = "db006";

//go:embed fixed/DB007.sql
const std::string DB007Scheme = sqlFiles.at("fixed/DB007.sql");

const std::string DB007 = "db007";

//go:embed fixed/DB008.sql
const std::string DB008Scheme = sqlFiles.at("fixed/DB008.sql");

const std::string DB008 = "db008";

//go:embed fixed/DB009.sql
const std::string DB009Scheme = sqlFiles.at("fixed/DB009.sql");

const std::string DB009 = "db009";

//go:embed fixed/DB010.sql
const std::string DB010Scheme = sqlFiles.at("fixed/DB010.sql");

const std::string DB010 = "db010";

//go:embed fixed/DB011.sql
const std::string DB011Scheme = sqlFiles.at("fixed/DB011.sql");

const std::string DB011 = "db011";

//go:embed fixed/DB013.sql
const std::string DB013Scheme = sqlFiles.at("fixed/DB013.sql");

const std::string DB013 = "db013";

//go:embed fixed/DB019.sql
const std::string DB019Scheme = sqlFiles.at("fixed/DB019.sql");

const std::string DB019 = "db019";

//go:embed fixed/DB020.sql
const std::string DB020Scheme = sqlFiles.at("fixed/DB020.sql");

const std::string DB020 = "db020";

//go:embed fixed/DB021.sql
const std::string DB021Scheme = sqlFiles.at("fixed/DB021.sql");

const std::string DB021 = "db021";

//go:embed fixed/DB022.sql
const std::string DB022Scheme = sqlFiles.at("fixed/DB022.sql");

const std::string DB022 = "db022";

//go:embed fixed/DB023.sql
const std::string DB023Scheme = sqlFiles.at("fixed/DB023.sql");

const std::string DB023 = "db023";

//go:embed fixed/DB024.sql
const std::string DB024Scheme = sqlFiles.at("fixed/DB024.sql");

const std::string DB024 = "db024";

//go:embed fixed/DB025.sql
const std::string DB025Scheme = sqlFiles.at("fixed/DB025.sql");

const std::string DB025 = "db025";

//go:embed fixed/DB025-Update.sql
const std::string DB025UpdateScheme = sqlFiles.at("fixed/DB025-Update.sql");

//go:embed fixed/DB026.sql
const std::string DB026Scheme = sqlFiles.at("fixed/DB026.sql");

const std::string DB026 = "db026";

//go:embed fixed/DB027.sql
const std::string DB027Scheme = sqlFiles.at("fixed/DB027.sql");

const std::string DB027 = "db027";

//go:embed fixed/DB028.sql
const std::string DB028Scheme = sqlFiles.at("fixed/DB028.sql");

const std::string DB028 = "db028";

//go:embed fixed/FSDB001.sql
const std::string FSDB001Scheme = sqlFiles.at("fixed/FSDB001.sql");

const std::string FSDB001 = "fsdb001";

//go:embed fixed/FSDB002.sql
const std::string FSDB002Scheme = sqlFiles.at("fixed/FSDB002.sql");

const std::string FSDB002 = "fsdb002";

// This is sentinel DB
// If this exists then we have successfully initialized all the DBs
//
//go:embed fixed/sentinel.sql
const std::string SentinelDBScheme = sqlFiles.at("fixed/sentinel.sql");

const std::string SentinelDB = "sentinel";


#endif