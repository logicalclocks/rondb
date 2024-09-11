# TRANTOR
[![Build Ubuntu gcc](../../actions/workflows/ubuntu-gcc.yml/badge.svg)](../../actions/workflows/ubuntu-gcc.yml/badge.svg)
[![Build Macos clang](../../actions/workflows/macos-clang.yml/badge.svg)](../../actions/workflows/macos-clang.yml/badge.svg)
[![Build RockyLinux gcc](../../actions/workflows/rockylinux-gcc.yml/badge.svg)](../../actions/workflows/rockylinux-gcc.yml/badge.svg)
[![Build Windows msvc](../../actions/workflows/windows-msvc.yml/badge.svg)](../../actions/workflows/windows-msvc.yml/badge.svg)

## Overview
A non-blocking I/O cross-platform TCP network library, using C++14.  
Drawing on the design of Muduo Library

## Supported platforms
- Linux
- MacOS
- UNIX(BSD)
- Windows

## Feature highlights
- Non-blocking I/O
- cross-platform
- Thread pool
- Lock free design
- Support SSL
- Server and Client


## Build
```shell
git clone https://github.com/an-tao/trantor.git
cd trantor
cmake -B build -H.
cd build 
make -j
```

## Licensing
Trantor - A non-blocking I/O based TCP network library, using C++14. 

Copyright (c) 2016-2021, Tao An.  All rights reserved.

https://github.com/an-tao/trantor

For more information see [License](License)

## Community
[Gitter](https://gitter.im/drogon-web/community)

## Documentation
[DocsForge](https://trantor.docsforge.com/)
