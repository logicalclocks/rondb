# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [1.5.21] - 2024-09-10

### API changes list

- Add a method to reload the SSL certificate and private key on the fly.

### Changed

- Keep log level consistency.

## [1.5.20] - 2024-07-20

### Changed

- Add byte order detection for internal SHA1 implementation for OSX, POWER, RISC-V and s390.

### Fixed

- Fix Windows CI build fail by using the latest MSVC.

- Fix the Botan TLS provider build on Linux.

- Fix "pthread not found" build error when using Android NDK.

## [1.5.19] - 2024-06-08

### changed

- show forked repository build status.

- Add cmake-format.

- Some spelling corrections.

## [1.5.18] - 2024-05-04

### Fixed

- Fix data type conflict.

- Fix build on latest c-ares.

## [1.5.17] - 2024-02-09

### Changed

- Make FileBufferNodeWin aware of UWP Win32 API.

- Use ssize_t declared by toolchain when available.

## [1.5.16] - 2024-01-18

### Changed

- Add build badge for individual OS.

- deinit libressl.

- Remove mutex.

### Fixed

- Pile of fix for h2.

- Fix a bug when sending data.

- Fix c-ares CARES_EXTERN for static builds.

- Fix header file name issue when cross-compiling on Windows.

- Fix name issue when cross-compiling.

## [1.5.15] - 2023-11-27

### Changed

- Feature: Integrate spdlog as logging backend for Trantor Logger.

### Fixed

- Fix the botan backend always validating certificate and OpenSSL allowing empty ALPN.

- Fix build error on OpenBSD.

- Fix Botan leaking memory if connection force closed.

- Fix a cmake warning.

- Workaround botan backend init failure on MacOS.

- Fix failing wstr conversion if locale is set to C.

## [1.5.14] - 2023-09-19

### [Fixed]

- Fix OpenSSL: read can be incomplete.

- Fix botan provider.

- Fix botan3 not triggering handshake finish event.

- Fix an compilation error when no STL lib is found.

## [1.5.13] - 2023-08-23

### Fixed

- Fix an error when sending files.

- Include &lt;memory&gt; header in TcpConnectionImpl.cc.

## [1.5.12] - 2023-08-20

### API changes list

- Add NetEndian versions of toIp and toIpPort.

- Add setsockopt to TcpClient and TcpServer.

- Support setting max files in AsyncFileLogger.

- Support returning multiple results for dns parsing.

### Changed

- Refactor SSL handling.

- Add ability to use one log file until the size-limit.

- Make the std::string_view work on windows.

- Drop Botan 2 support and support Botan 3.

- Make the getNextLoop method multi-thread safe.

- Add fallback when OpenSSL not providing BLAKE2b.

### Fixed

- Fix override mark.

- Add missing &lt;cstdint&gt; header with GCC 13.

- Fix AresResolver.

- Fix building built-in hashes on Windows.

- Fix MSYS2/Cygwin compatibility issues.

- Fix more build errors on win32/mingw.

- Fix off_t(on windows off_t defined with long, not longlong).

- Fix bug with Trantor::Date timeZoneOffset calculation.

- Fix wrong usage of shared pointer in TcpClient ctor.

## [1.5.11] - 2023-03-17

### API Changes list

- Add a method to the Logger class to enable local time displaying.

- TRNANTOR_LOG_COMPACT - compact logs without source code details.

### Changed

- Refactor TcpServer I/O loop logic.

### Fixed

- Fix a conan issue.

## [1.5.10] - 2023-01-23

### API Changes list

### Changed

- Use gtest 1.13 in github actions

### Fixed

## [1.5.9] - 2023-01-23

### API Changes list

### Changed

- Search for \ if under msvc

### Fixed

## [1.5.8] - 2022-11-11

### API Changes list

### Changed

### Fixed

- Fix Date::timezoneOffset().

- Fix socket fd leak if Connector destruct before connection callback is made.

## [1.5.7] - 2022-09-25

### API changes list

- Add utc methods for trantor::Date.

### Changed

- Remove an unnecessary semi-colon.

- Added support for Solaris.

- Define ssize_t as std::intptr_t on Windows.

- Add an environment without openssl to github actions.

- Added SSL Error Trace Log and mTLS Samples.

- Use LOG_TRACE instead of LOG_DEBUG.

### Fixed

- Fix a race condition.

- Fix iterator invalidation bug when stopping TCP server.

- Partial fix of exception safety in the event loop.

## [1.5.6] - 2022-07-09

### API changes list

- Add support for sending data streams via callback.

- Added mTLS support.

### Changed

- Make MsgBuffer constructor explicit.

- Always queue connectDestroyed() in loop.

- Stop calling abort() in runtime.

- Give EventLoopThread::loop_ static lifetime.

- Optimization SSL name matching.

- Clarify SSL error message.

- Rename BUILD_TRANTOR_SHARED to BUILD_SHARED_LIBS.

### Fixed

- Fix tolower with sanitizer cfi

- include <pthread_np.h> unconditionally on freebsd

- Fix thread sanitizer.

## [1.5.5] - 2022-02-19

### API changes list

### Changed

- Move EventLoop::runAfter to a template.

- Remove an assertion when removing channels.

- Prevent TcpClient::removeConnection call on deleted TcpClient instance.

- Wait for loop to exit in EventLoop destructor.

- Add r-reference version of set-callback methods to TcpConnectionImpl.

### Fixed

- Fix a bug when closing connections on Windows/MacOS.

- Fix logger causes if statement mismatch.

## [1.5.4] - 2021-12-10

### API changes list

### Changed

- Correctly handle the error of the getaddrinfo function

### Fixed

- Fix the error when sending partial files

## [1.5.3] - 2021-11-28

### API changes list

- TcpClientImpl support SSL client certificate

### Changed

- Allow RVO in fromDbStringLocal

### Fixed

- Make sure resolvers are added when C-Ares is manually disabled

## [1.5.2] - 2021-10-17

### API changes list

### Changed

- Disable setting SSL Configs when using LibreSSL

- cmake: Use GNUInstallDirs to figure out install dirs

- support HaikuOS

- Improve Error handling for certificates/private keys

- Make c-ares support optional when building

- Use locale.h

- Assert fd >= 0 in updateChannel()

- Add Clang support for -Wall -Wextra -Werror; fix -Wunused-parameter

### Fixed

- Fix a small memory leak

- Fix errors in log macros

- Fix a race condition when TcpClient destroyed before connected

- Fix the error of calling removeAndResetChannel twice

- Fix a bug when EAGAIN on reading sockets

- Fix compilation warnings

- Fix a potential race condition

## [1.5.1] - 2021-08-08

### API changes list

### Changed

- Fix warning C4244 in MSVC

- Disable strict compiler check on Windows with GCC

- Add support for paths containing unicode characters on Windows

- Add BUILD_DOC cmake option (doxygen)

- Use make_shared instead of shared_ptr(new)

- Detect and handle MinGW

### Fixed

- Fix the destructor of AresResolver

- Fix memory leak in NormalResolver

## [1.5.0] - 2021-06-18

### API changes list

- Enable multiple log files or streams.

- Add SSL_CONF_cmd support.

- Add runOnQuit to the EventLoop class.

### Changed

- Export the FixedBuffer.

- Added a try_compile block to detect if we need to link against atomic.

## [1.4.1] - 2021-05-15

### Changed

- Add github actions of Windows.

- Modify the way the log file is opened.

- Add version/soversion to shared library.

- Use double instead of long double as the type for timer durations.

### Fixed

- Fix a bug in the TcpConnectionImpl class.

- Fix constructing Date in a daylight saving timezone.

- GNU: -Wall -Wextra -Werror; fix related warnings.

- Add wincrypt.h include for Windows.

## [1.4.0] - 2021-04-09

### API changes list

- Add isUnspecified() to indicate if IP parsing failed.

- Add exports macro to allow Shared Library with hidden symbols by default.

### Changed

- Modify the AsyncFileLogger destructor.

### Fixed

- Recycle TimerID in the TimerQueue.

## [1.3.0] - 2021-03-05

### API changes list

- Add secondsSinceEpoch to trantor::Date.

- Rename the 'bzero' method of the FixedBuffer class to 'zeroBuffer'.

- Add SNI support to TcpClient.

- Add SSL certificate validation.

### Changed

- Change README.md.

## [1.2.0] - 2021-01-16

### API changes list

- Add LOG_IF and DLOG like glog lib.

### Changed

- Enable github actions.

- Add support for VS2019.

- Modify the LockFreeQueue.

### Fixed

- Fix MinGW error with inet_ntop and inet_pton.

- Fix a macro regression when using MSVC.

## [1.1.1] - 2020-12-12

### Changed

- Add Openbsd support.

## [1.1.0] - 2020-10-24

### Changed

- Disable TLS 1.0 and 1.1 by default.

- Use explicit lambda capture lists.

### Fixed

- Fix a bug in the Date::fromDbStringLocal() method.

## [1.0.0] - 2020-9-27

### API changes list

- Add the address() method to the TcpServer class.

- Change some internal methods from public to private in the Channel class.

### Changed

- Update the wepoll library.

- Add comments in public header files.

## [1.0.0-rc16] - 2020-8-15

### Fixed

- Fix a bug when sending big files on Windows.

### API changes list

- Add updateEvents() method to the Channel class.

## [1.0.0-rc15] - 2020-7-16

### Fixed

- Fix installation errors of shared library.

## [1.0.0-rc14] - 2020-6-14

### API changes list

- Add the moveToCurrentThread() method to EventLoop.

### Changed

- Optimized LockFreeQueue by Reducing Object Construction.

### Fixed

- Fix a bug when sending a file.

## [1.0.0-rc13] - 2020-5-23

### API changes list

- Make the Channel class as a part of the public API.

## [1.0.0-rc12] - 2020-5-22

### API changes list

- Add a method to show if the c-ares library is used

### Fixed

- Fix a bug in SSL mode (#85)

- Use SOCKET type in windows for x86-windows compilation

- Use env to find bash in build.sh script to support FreeBSD

## [1.0.0-rc11] - 2020-4-27

### API changes list

- Add fromDbStringLocal() method to the Date class

### Fixed

- Fix a race condition of TimingWheel class

- Fix localhost resolving on windows

## [1.0.0-rc10] - 2020-3-28

### API changes list

- Add the send(const void *, size_t) method to the TcpConnection class

- Add the send(const MsgBufferPtr &) method to TcpConnection class

- Add stop() method to the TcpServer class

### Changed

- Compile wepoll directly into trantor (Windows)

- Add CI for Windows

- Make CMake install files relocatable

- Modify the Resolver class

## [1.0.0-rc9] - 2020-2-17

### API changes list

- Add support for a delayed SSL handshake

- Change a method name of EventLoopThreadPool(getLoopNum() -> size())

### Changed

- Port Trantor to Windows

- Use SSL_CTX_use_certificate_chain_file instead of SSL_CTX_use_certificate_file()

## [1.0.0-rc8] - 2019-11-30

### API changes list

- Add the isSSLConnection() method to the TcpConnection class

### Changed

- Use the std::chrono::steady_clock for timers

## [1.0.0-rc7] - 2019-11-21

### Changed

- Modify some code styles

## [1.0.0-rc6] - 2019-10-4

### API changes list

- Add index() interface to the EventLoop class.

### Changed

- Fix some compilation warnings.

- Modify the CMakeLists.txt

## [1.0.0-rc5] - 2019-08-24

### API changes list

- Remove the resolve method from the InetAddress class.

### Added

- Add the Resolver class that provides high-performance DNS functionality(with c-ares library)
- Add some unit tests.

## [1.0.0-rc4] - 2019-08-08

### API changes list

- None

### Changed

- Add TrantorConfig.cmake so that users can use trantor with the `find_package(Trantor)` command.

### Fixed

- Fix an SSL error (occurs when sending large data via SSL).

## [1.0.0-rc3] - 2019-07-30

### API changes list

- TcpConnection::setContext, TcpConnection::getContext, etc.
- Remove the config.h from public API.

### Changed

- Modify the CMakeLists.txt.
- Modify some log output.
- Remove some unnecessary `std::dynamic_pointer_cast` calls.

## [1.0.0-rc2] - 2019-07-11

### Added

- Add bytes statistics methods to the TcpConnection class.
- Add the setIoLoopThreadPool method to the TcpServer class.

### Changed

- Ignore SIGPIPE signal when using the TcpClient class.
- Enable TCP_NODELAY by default (for higher performance).

## [1.0.0-rc1] - 2019-06-11

[Unreleased]: https://github.com/an-tao/trantor/compare/v1.5.21...HEAD

[1.5.21]: https://github.com/an-tao/trantor/compare/v1.5.20...v1.5.21

[1.5.20]: https://github.com/an-tao/trantor/compare/v1.5.19...v1.5.20

[1.5.19]: https://github.com/an-tao/trantor/compare/v1.5.18...v1.5.19

[1.5.18]: https://github.com/an-tao/trantor/compare/v1.5.17...v1.5.18

[1.5.17]: https://github.com/an-tao/trantor/compare/v1.5.16...v1.5.17

[1.5.16]: https://github.com/an-tao/trantor/compare/v1.5.15...v1.5.16

[1.5.15]: https://github.com/an-tao/trantor/compare/v1.5.14...v1.5.15

[1.5.14]: https://github.com/an-tao/trantor/compare/v1.5.13...v1.5.14

[1.5.13]: https://github.com/an-tao/trantor/compare/v1.5.12...v1.5.13

[1.5.12]: https://github.com/an-tao/trantor/compare/v1.5.11...v1.5.12

[1.5.11]: https://github.com/an-tao/trantor/compare/v1.5.10...v1.5.11

[1.5.10]: https://github.com/an-tao/trantor/compare/v1.5.9...v1.5.10

[1.5.9]: https://github.com/an-tao/trantor/compare/v1.5.8...v1.5.9

[1.5.8]: https://github.com/an-tao/trantor/compare/v1.5.7...v1.5.8

[1.5.7]: https://github.com/an-tao/trantor/compare/v1.5.6...v1.5.7

[1.5.6]: https://github.com/an-tao/trantor/compare/v1.5.5...v1.5.6

[1.5.5]: https://github.com/an-tao/trantor/compare/v1.5.4...v1.5.5

[1.5.4]: https://github.com/an-tao/trantor/compare/v1.5.3...v1.5.4

[1.5.3]: https://github.com/an-tao/trantor/compare/v1.5.2...v1.5.3

[1.5.2]: https://github.com/an-tao/trantor/compare/v1.5.1...v1.5.2

[1.5.1]: https://github.com/an-tao/trantor/compare/v1.5.0...v1.5.1

[1.5.0]: https://github.com/an-tao/trantor/compare/v1.4.1...v1.5.0

[1.4.1]: https://github.com/an-tao/trantor/compare/v1.4.0...v1.4.1

[1.4.0]: https://github.com/an-tao/trantor/compare/v1.3.0...v1.4.0

[1.3.0]: https://github.com/an-tao/trantor/compare/v1.2.0...v1.3.0

[1.2.0]: https://github.com/an-tao/trantor/compare/v1.1.1...v1.2.0

[1.1.1]: https://github.com/an-tao/trantor/compare/v1.1.0...v1.1.1

[1.1.0]: https://github.com/an-tao/trantor/compare/v1.0.0...v1.1.0

[1.0.0]: https://github.com/an-tao/trantor/compare/v1.0.0-rc16...v1.0.0

[1.0.0-rc16]: https://github.com/an-tao/trantor/compare/v1.0.0-rc15...v1.0.0-rc16

[1.0.0-rc15]: https://github.com/an-tao/trantor/compare/v1.0.0-rc14...v1.0.0-rc15

[1.0.0-rc14]: https://github.com/an-tao/trantor/compare/v1.0.0-rc13...v1.0.0-rc14

[1.0.0-rc13]: https://github.com/an-tao/trantor/compare/v1.0.0-rc12...v1.0.0-rc13

[1.0.0-rc12]: https://github.com/an-tao/trantor/compare/v1.0.0-rc11...v1.0.0-rc12

[1.0.0-rc11]: https://github.com/an-tao/trantor/compare/v1.0.0-rc10...v1.0.0-rc11

[1.0.0-rc10]: https://github.com/an-tao/trantor/compare/v1.0.0-rc9...v1.0.0-rc10

[1.0.0-rc9]: https://github.com/an-tao/trantor/compare/v1.0.0-rc8...v1.0.0-rc9

[1.0.0-rc8]: https://github.com/an-tao/trantor/compare/v1.0.0-rc7...v1.0.0-rc8

[1.0.0-rc7]: https://github.com/an-tao/trantor/compare/v1.0.0-rc6...v1.0.0-rc7

[1.0.0-rc6]: https://github.com/an-tao/trantor/compare/v1.0.0-rc5...v1.0.0-rc6

[1.0.0-rc5]: https://github.com/an-tao/trantor/compare/v1.0.0-rc4...v1.0.0-rc5

[1.0.0-rc4]: https://github.com/an-tao/trantor/compare/v1.0.0-rc3...v1.0.0-rc4

[1.0.0-rc3]: https://github.com/an-tao/trantor/compare/v1.0.0-rc2...v1.0.0-rc3

[1.0.0-rc2]: https://github.com/an-tao/trantor/compare/v1.0.0-rc1...v1.0.0-rc2

[1.0.0-rc1]: https://github.com/an-tao/trantor/releases/tag/v1.0.0-rc1
