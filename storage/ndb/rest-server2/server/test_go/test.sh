set -e

export RDRS_CONFIG_FILE=/Users/salman/code/hops/rondb2/build/mysql-test/var/rdrs.1.1_config.json

./script.sh test  
#./script.sh restart
#./script.sh test  hopsworks.ai/rdrs2/internal/integrationtests/batchfeaturestore Test_GetFeatureVector_Success_ComplexType_ST
#./script.sh test  hopsworks.ai/rdrs2/internal/integrationtests/feature_store Test
#./script.sh test  hopsworks.ai/rdrs2/internal/integrationtests/batchfeaturestore Test
#                                                                             Test_GetFeatureVector_Success_ComplexType_ST
#./script.sh test  hopsworks.ai/rdrs2/internal/integrationtests/batchfeaturestore Test_GetFeatureVector_Success_ComplexType_512

#./script.sh restart
#./script.sh test hopsworks.ai/rdrs2/internal/integrationtests/pkread TestDataTypesBlob/simple2

#./script.sh test hopsworks.ai/rdrs2/internal/integrationtests/feature_store Test_CAPS_Proj_Name

#./script.sh test  hopsworks.ai/rdrs2/internal/integrationtests/batchfeaturestore  Test_GetFeatureVector_CacheFG_5entries_Metadata_Success
