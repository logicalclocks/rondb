cd /build
tar xfz rondb-gpl-${RONDB_VERSION}.tar.gz
cd rondb-gpl-${RONDB_VERSION}
if test "x${BUILD_RELEASE}" = "xyes" ; then
  mkdir feedback_build
  cd feedback_build
  /build_scripts/build_gen.sh
  make -j 32
  cd mysql-test
  /build_scripts/run_training.sh
  cd /build/rondb-gpl-${RONDB_VERSION}
  mkdir use_build
  cd use_build
  /build_scripts/build_use.sh
  make -j 32
  make install
else
  mkdir build_dir
  cd build_dir
  /build_scripts/build_prefix.sh
  make -j 32
  make install
fi
cd /build
rm -rf rondb-gpl-${RONDB_VERSION}
/build_scripts/build_bench.sh
cd /build
/build_scripts/copy_bench_files.sh
cd /build
/build_scripts/create_rondb_tarball.sh
