SYSBENCH_VERSION="sysbench-0.4.12.17"
DBT2_VERSION="dbt2-0.37.50.18"
DBT3_VERSION="dbt3-1.10"
rm -rf ${SYSBENCH_VERSION}
tar xfz ${SYSBENCH_VERSION}.tar.gz
cd ${SYSBENCH_VERSION}
./configure --with-mysql=/build/rondb_bin_use
make
cd ..
rm -rf ${DBT2_VERSION}
tar xfz ${DBT2_VERSION}.tar.gz
cd ${DBT2_VERSION}
./configure --with-mysql=/build/rondb_bin_use
make
cd ..
rm -rf ${DBT3_VERSION}
tar xfz ${DBT3_VERSION}.tar.gz
cd ${DBT3_VERSION}
./configure --with-mysql=/build/rondb_bin_use
make
cd ..
