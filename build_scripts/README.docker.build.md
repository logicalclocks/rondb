# Building RonDB with Docker

Building RonDB with Docker is possible with two different Dockerfiles: 

* [Dockerfile.oraclelinux8](../Dockerfile.oraclelinux8)
* [Dockerfile.oraclelinux9](../Dockerfile.oraclelinux9)
* [Dockerfile.ubuntu22](../Dockerfile.ubuntu22)

At Hopsworks, we use `Dockerfile.oraclelinux8` for our production builds in CentOS environment and `Dockerfile.oraclelinux9´ for our x86 production builds in Ubuntu environments. The `Dockerfile.ubuntu22` on the other hand is meant for experimental testing with ARM64.

To build RonDB with the aim of **extracting the tarball**, use the Dockerfiles as follows:
```bash
BUILD_CORES=$(nproc)  # Default will be 1

# Run this from the root directory of the repository
BUILDKIT_ENABLED=1 docker build . \
    -f Dockerfile.oraclelinux8 \
    --target get-package-all \
    --output <local-path-to-place-tarball> \
    --build-arg BUILD_THREADS=$BUILD_CORES \  # To accelerate the build
    --build-arg RELEASE_TARBALL=1  # Omit this entirely to create a simple build
```

This will create the RonDB tarball as part of the Docker build process. It will also use Docker's mounted build caches to save the intermediate binaries. See the statements such as `--mount=type=cache,target=rondb-bin,id=ubuntu22-rondb2210-bin` in the Dockerfile. These will largely accelerate consecutive builds. To clear the cache, run `docker builder prune`. You can also add the flag `--no-cache` to the Docker build command to create an absolutely clean build.

If you want to build RDRS docker image than run the following command

# Run this from the root directory of the repository
BUILDKIT_ENABLED=1 docker build . \
    -f Dockerfile.oraclelinux7 \  # Or Dockerfile.ubuntu22 for ARM64
    --target rdrs \
    --tag rdrs:22.10 \ # Update the tag and version
    --build-arg BUILD_THREADS=$BUILD_CORES \  # To accelerate the build
    --build-arg RELEASE_TARBALL=1  # Omit this entirely to create a simple build

Use the `docker images` and `docker save` commands to list and save docker images

If you wish to build RonDB manually inside a container, you can use [docker-create-builder.sh](/build_scripts/release_scripts/docker-create-builder.sh). This can be useful if you want to try different build configurations whilst developing RonDB. The build files are persisted inside a volume and the source code is mounted into the container. The script finishes by opening a shell inside the container.
