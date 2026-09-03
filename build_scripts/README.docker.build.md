# Building RonDB with Docker

Building RonDB with Docker is possible with three different Dockerfiles:

* [Dockerfile.oraclelinux8](../Dockerfile.oraclelinux8)
* [Dockerfile.oraclelinux9](../Dockerfile.oraclelinux9)
* [Dockerfile.ubuntu22](../Dockerfile.ubuntu22)

At Hopsworks, we use `Dockerfile.oraclelinux9` for our production builds in CentOS environment and `Dockerfile.ubuntu22´ for our x86 production builds in Ubuntu environments. All Dockerfiles can be used to build both x86_64 and ARM64 binary tarballs.

To build RonDB with the aim of **extracting the tarball**, use the Dockerfiles as follows:
```bash
# Run this from the root directory of the repository
# Use any of the above Dockerfiles whether running this on an x86\_64 or ARM64 platform
# Omit the RELEASE_TARBALL argument to create a simple build
BUILDKIT_ENABLED=1 docker build . \
    -f Dockerfile.oraclelinux9 \
    --target get-package-all \
    --output <local-path-to-place-tarball> \
    --build-arg BUILD_THREADS=$(nproc) \
    --build-arg RELEASE_TARBALL=1
```

This will create the RonDB tarball as part of the Docker build process. It will also use Docker's mounted build caches to save the intermediate binaries. See the statements such as `--mount=type=cache,target=rondb-bin,id=ubuntu22-rondb2210-bin` in the Dockerfile. These will largely accelerate consecutive builds. To clear the cache, run `docker builder prune`. You can also add the flag `--no-cache` to the Docker build command to create an absolutely clean build.

# Building RDRS docker image
Run the following command from the repository root directory

```
# Update the tag and version
# Omit the RELEASE_TARBALL argument to create a simple build
BUILDKIT_ENABLED=1 docker build . \
    -f Dockerfile.oraclelinux9 \
    --target rdrs \
    --tag rdrs:24.10 \
    --build-arg BUILD_THREADS=$(nproc) \
    --build-arg RELEASE_TARBALL=1
```

Use the `docker images` and `docker save` commands to list and save docker images

The builds in the mounted caches are however difficult to access. If you wish to access the builds themselves, it is recommended to use the script [docker-build.sh](/build_scripts/release_scripts/docker-build.sh). This will use the same Dockerfiles, but instead of building RonDB inside the Docker build process, it will build RonDB inside a running RonDB container. This makes it easier to access the builds.

If you wish to build RonDB manually inside a container, you can use [docker-create-builder.sh](/build_scripts/release_scripts/docker-create-builder.sh). This can be useful if you want to try different build configurations whilst developing RonDB. The build files are persisted inside a volume and the source code is mounted into the container. The script finishes by opening a shell inside the container.
