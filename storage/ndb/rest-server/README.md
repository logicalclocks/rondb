# RDRS -- *R*on*D*B *R*EST API *S*erver 

This RonDB REST API Server and data access library. 

## Running unit tests via Docker

In case you are developing inside a Docker container and you wish to run unit tests against a RonDB cluster, you can choose between

1. RonDB running as part of MTR tests
2. RonDB running as a separate Docker Compose cluster

In case you decide for the latter, you can spin up a cluster using [rondb-docker](https://github.com/logicalclocks/rondb-docker). When a Docker Compose cluster is running, the container you are running from has to be added to the network of this cluster. Do so by running the following commands:

```bash
docker network list  # Search for the network name of the Docker Compose cluster
docker network connect <cluster network> <development container id>
```

To make sure that the unit tests will be run against the correct IPs, adjust them in [config.go](/storage/ndb/rest-server/rest-api-server/internal/config/config.go). You will be required to reference the IP of the mgmd and the MySQLd - you can however simply reference the container names of these services. Check the rondb-docker repository for the MySQL user and password to use.

Lastly, RonDB must be built entirely for your unit tests to run. This is because we rely on the ndb api client library, which is a dynamic C library. Thus, run your build command with CMake next. It is however important that CMake receieves the `-DWITH_RDRS=1` parameter.

Assuming that you have built into the directory `/root/rondb-bin`, you must then run the following commands:

```bash
export LD_LIBRARY_PATH=/root/rondb-bin/lib
ldconfig
```

Finally, you can run the unit tests by entering the [rest-api-server](/storage/ndb/rest-server/rest-api-server) directory and running `make test`.
