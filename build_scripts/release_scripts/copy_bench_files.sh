cd rondb_bin_use
cd bin
SYSBENCH_VERSION="sysbench-0.4.12.17"
DBT2_VERSION="dbt2-0.37.50.18"
DBT3_VERSION="dbt3-1.10"
mkdir dbt2
mkdir sysbench
mkdir dbt3
cp /build/${SYSBENCH_VERSION}/sysbench/sysbench sysbench/sysbench
cp /build/${DBT2_VERSION}/src/client dbt2/client
cp /build/${DBT2_VERSION}/src/driver dbt2/driver
cp /build/${DBT2_VERSION}/src/datagen dbt2/datagen
cp /build/${DBT2_VERSION}/scripts/bench_run.sh bench_run.sh
cp /build/${DBT3_VERSION}/src/dbgen/dbgen dbt3/dbgen
cp /build/${DBT3_VERSION}/src/dbgen/qgen dbt3/qgen
cd /build
rm -rf ${SYSBENCH_VERSION}
rm -rf ${DBT2_VERSION}
rm -rf ${DBT3_VERSION}
tar xfz ${DBT2_VERSION}.tar.gz
mv ${DBT2_VERSION} rondb_bin_use/dbt2_install
