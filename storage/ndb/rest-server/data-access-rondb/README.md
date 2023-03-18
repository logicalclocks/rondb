# Data access layer used by RonDB REST API Server 

## Building

To test building ***only*** this directory, you must first let CMake create the Makefile. To do so, 

1. Create a build directory, e.g. `/tmp/rondb-bin` and navigate into it
2. Run one of the CMake commands to be seen at the top of every one of the [build_scripts](/build_scripts). Make sure that the flag `-DWITH_RDRS=1` is set. If it is not, simply append it to the other CMake flags.

Now, you can build the directory by running the following:
```bash
cd /tmp/rondb-bin/storage/ndb/rest-server/data-access-rondb
make -j10
```
