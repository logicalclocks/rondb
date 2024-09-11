/**
 *
 *  @file Utilities.cc
 *  @author An Tao
 *
 *  Copyright 2018, An Tao.  All rights reserved.
 *  https://github.com/an-tao/drogon
 *  Use of this source code is governed by a MIT license
 *  that can be found in the License file.
 *
 *  Drogon
 *
 */

#include "Utilities.h"
#ifdef _WIN32
#include <windows.h>
#include <ntsecapi.h>
#include <algorithm>
#else  // _WIN32
#include <unistd.h>
#include <string.h>
#if __cplusplus < 201103L || __cplusplus >= 201703L
#include <stdlib.h>
#include <locale.h>
#else  // __cplusplus
#include <locale>
#include <codecvt>
#endif  // __cplusplus
#endif  // _WIN32

#if defined(USE_OPENSSL)
#include <openssl/rand.h>
#include <limits>
#elif defined(USE_BOTAN)
#include <botan/auto_rng.h>
#else
#include "crypto/md5.h"
#include "crypto/sha1.h"
#include "crypto/sha256.h"
#include "crypto/sha3.h"
#include "crypto/blake2.h"
#include <fstream>
#include <chrono>
#include <random>
#endif

#ifdef _MSC_VER
#include <intrin.h>
#else
#if defined(__x86_64__) || defined(__i386__)
#include <x86intrin.h>
#endif
#endif

#include <cassert>
#include <functional>
#include <memory>
#include <trantor/utils/Logger.h>

#if __cplusplus < 201103L || __cplusplus >= 201703L
static std::wstring utf8Toutf16(const std::string &utf8Str)
{
    std::wstring utf16Str;
    utf16Str.reserve(utf8Str.length());  // Reserve space to avoid reallocations

    for (size_t i = 0; i < utf8Str.length();)
    {
        wchar_t unicode_char;

        // Check the first byte
        if ((utf8Str[i] & 0b10000000) == 0)
        {
            // Single-byte character (ASCII)
            unicode_char = utf8Str[i++];
        }
        else if ((utf8Str[i] & 0b11100000) == 0b11000000)
        {
            if (i + 1 >= utf8Str.length())
            {
                // Invalid UTF-8 sequence
                // Handle the error as needed
                return L"";
            }
            // Two-byte character
            unicode_char = ((utf8Str[i] & 0b00011111) << 6) |
                           (utf8Str[i + 1] & 0b00111111);
            i += 2;
        }
        else if ((utf8Str[i] & 0b11110000) == 0b11100000)
        {
            if (i + 2 >= utf8Str.length())
            {
                // Invalid UTF-8 sequence
                // Handle the error as needed
                return L"";
            }
            // Three-byte character
            unicode_char = ((utf8Str[i] & 0b00001111) << 12) |
                           ((utf8Str[i + 1] & 0b00111111) << 6) |
                           (utf8Str[i + 2] & 0b00111111);
            i += 3;
        }
        else
        {
            // Invalid UTF-8 sequence
            // Handle the error as needed
            return L"";
        }

        utf16Str.push_back(unicode_char);
    }

    return utf16Str;
}

static std::string utf16Toutf8(const std::wstring &utf16Str)
{
    std::string utf8Str;
    utf8Str.reserve(utf16Str.length() * 3);

    for (size_t i = 0; i < utf16Str.length(); ++i)
    {
        wchar_t unicode_char = utf16Str[i];

        if (unicode_char <= 0x7F)
        {
            // Single-byte character (ASCII)
            utf8Str.push_back(static_cast<char>(unicode_char));
        }
        else if (unicode_char <= 0x7FF)
        {
            // Two-byte character
            utf8Str.push_back(
                static_cast<char>(0xC0 | ((unicode_char >> 6) & 0x1F)));
            utf8Str.push_back(static_cast<char>(0x80 | (unicode_char & 0x3F)));
        }
        else
        {
            // Three-byte character
            utf8Str.push_back(
                static_cast<char>(0xE0 | ((unicode_char >> 12) & 0x0F)));
            utf8Str.push_back(
                static_cast<char>(0x80 | ((unicode_char >> 6) & 0x3F)));
            utf8Str.push_back(static_cast<char>(0x80 | (unicode_char & 0x3F)));
        }
    }

    return utf8Str;
}
#endif  // __cplusplus

namespace trantor
{
namespace utils
{
std::string toUtf8(const std::wstring &wstr)
{
    if (wstr.empty())
        return {};

    std::string strTo;
#ifdef _WIN32
    int nSizeNeeded = ::WideCharToMultiByte(
        CP_UTF8, 0, &wstr[0], (int)wstr.size(), NULL, 0, NULL, NULL);
    strTo.resize(nSizeNeeded, 0);
    ::WideCharToMultiByte(CP_UTF8,
                          0,
                          &wstr[0],
                          (int)wstr.size(),
                          &strTo[0],
                          nSizeNeeded,
                          NULL,
                          NULL);
#elif __cplusplus < 201103L || __cplusplus >= 201703L
    strTo = utf16Toutf8(wstr);
#else  // c++11 to c++14
    std::wstring_convert<std::codecvt_utf8<wchar_t>, wchar_t> utf8conv;
    strTo = utf8conv.to_bytes(wstr);
#endif
    return strTo;
}
std::wstring fromUtf8(const std::string &str)
{
    if (str.empty())
        return {};
    std::wstring wstrTo;
#ifdef _WIN32
    int nSizeNeeded =
        ::MultiByteToWideChar(CP_UTF8, 0, &str[0], (int)str.size(), NULL, 0);
    wstrTo.resize(nSizeNeeded, 0);
    ::MultiByteToWideChar(
        CP_UTF8, 0, &str[0], (int)str.size(), &wstrTo[0], nSizeNeeded);
#elif __cplusplus < 201103L || __cplusplus >= 201703L
    wstrTo = utf8Toutf16(str);
#else  // c++11 to c++14
    std::wstring_convert<std::codecvt_utf8<wchar_t>, wchar_t> utf8conv;
    try
    {
        wstrTo = utf8conv.from_bytes(str);
    }
    catch (...)  // Should never fail if str valid UTF-8
    {
    }
#endif
    return wstrTo;
}

std::wstring toWidePath(const std::string &strUtf8Path)
{
    auto wstrPath{fromUtf8(strUtf8Path)};
#ifdef _WIN32
    // Not needed: normalize path (just replaces '/' with '\')
    std::replace(wstrPath.begin(), wstrPath.end(), L'/', L'\\');
#endif  // _WIN32
    return wstrPath;
}

std::string fromWidePath(const std::wstring &wstrPath)
{
#ifdef _WIN32
    auto srcPath{wstrPath};
    // Not needed: to portable path (just replaces '\' with '/')
    std::replace(srcPath.begin(), srcPath.end(), L'\\', L'/');
#else   // _WIN32
    auto &srcPath{wstrPath};
#endif  // _WIN32
    return toUtf8(srcPath);
}

bool verifySslName(const std::string &certName, const std::string &hostname)
{
    if (certName.find('*') == std::string::npos)
    {
        return certName == hostname;
    }

    size_t firstDot = certName.find('.');
    size_t hostFirstDot = hostname.find('.');
    size_t pos, len, hostPos, hostLen;

    if (firstDot != std::string::npos)
    {
        pos = firstDot + 1;
    }
    else
    {
        firstDot = pos = certName.size();
    }

    len = certName.size() - pos;

    if (hostFirstDot != std::string::npos)
    {
        hostPos = hostFirstDot + 1;
    }
    else
    {
        hostFirstDot = hostPos = hostname.size();
    }

    hostLen = hostname.size() - hostPos;

    // *. in the beginning of the cert name
    if (certName.compare(0, firstDot, "*") == 0)
    {
        return certName.compare(pos, len, hostname, hostPos, hostLen) == 0;
    }
    // * in the left most. but other chars in the right
    else if (certName[0] == '*')
    {
        // compare if `hostname` ends with `certName` but without the leftmost
        // should be fine as domain names can't be that long
        intmax_t hostnameIdx = hostname.size() - 1;
        intmax_t certNameIdx = certName.size() - 1;
        while (hostnameIdx >= 0 && certNameIdx != 0)
        {
            if (hostname[hostnameIdx] != certName[certNameIdx])
            {
                return false;
            }
            hostnameIdx--;
            certNameIdx--;
        }
        if (certNameIdx != 0)
        {
            return false;
        }
        return true;
    }
    // * in the right of the first dot
    else if (firstDot != 0 && certName[firstDot - 1] == '*')
    {
        if (certName.compare(pos, len, hostname, hostPos, hostLen) != 0)
        {
            return false;
        }
        for (size_t i = 0;
             i < hostFirstDot && i < firstDot && certName[i] != '*';
             i++)
        {
            if (hostname[i] != certName[i])
            {
                return false;
            }
        }
        return true;
    }
    // else there's a * in  the middle
    else
    {
        if (certName.compare(pos, len, hostname, hostPos, hostLen) != 0)
        {
            return false;
        }
        for (size_t i = 0;
             i < hostFirstDot && i < firstDot && certName[i] != '*';
             i++)
        {
            if (hostname[i] != certName[i])
            {
                return false;
            }
        }
        intmax_t hostnameIdx = hostFirstDot - 1;
        intmax_t certNameIdx = firstDot - 1;
        while (hostnameIdx >= 0 && certNameIdx >= 0 &&
               certName[certNameIdx] != '*')
        {
            if (hostname[hostnameIdx] != certName[certNameIdx])
            {
                return false;
            }
            hostnameIdx--;
            certNameIdx--;
        }
        return true;
    }

    assert(false && "This line should not be reached in verifySslName");
    // should not reach
    return certName == hostname;
}

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

std::string tlsBackend()
{
    return TOSTRING(TRANTOR_TLS_PROVIDER);
}
#undef TOSTRING
#undef STRINGIFY

#if !defined(USE_BOTAN) && !defined(USE_OPENSSL)
Hash128 md5(const void *data, size_t len)
{
    MD5_CTX ctx;
    trantor_md5_init(&ctx);
    trantor_md5_update(&ctx, (const unsigned char *)data, len);
    Hash128 hash;
    trantor_md5_final(&ctx, (unsigned char *)&hash);
    return hash;
}

Hash160 sha1(const void *data, size_t len)
{
    SHA1_CTX ctx;
    trantor_sha1_init(&ctx);
    trantor_sha1_update(&ctx, (const unsigned char *)data, len);
    Hash160 hash;
    trantor_sha1_final((unsigned char *)&hash, &ctx);
    return hash;
}

Hash256 sha256(const void *data, size_t len)
{
    SHA256_CTX ctx;
    trantor_sha256_init(&ctx);
    trantor_sha256_update(&ctx, (const unsigned char *)data, len);
    Hash256 hash;
    trantor_sha256_final(&ctx, (unsigned char *)&hash);
    return hash;
}

Hash256 sha3(const void *data, size_t len)
{
    Hash256 hash;
    trantor_sha3((const unsigned char *)data, len, &hash, sizeof(hash));
    return hash;
}

Hash256 blake2b(const void *data, size_t len)
{
    Hash256 hash;
    trantor_blake2b(&hash, sizeof(hash), data, len, NULL, 0);
    return hash;
}
#endif

std::string toHexString(const void *data, size_t len)
{
    std::string str;
    str.resize(len * 2);
    for (size_t i = 0; i < len; i++)
    {
        unsigned char c = ((const unsigned char *)data)[i];
        str[i * 2] = "0123456789ABCDEF"[c >> 4];
        str[i * 2 + 1] = "0123456789ABCDEF"[c & 0xf];
    }
    return str;
}

#if !defined(USE_BOTAN) && !defined(USE_OPENSSL)
/**
 * @brief Generates `size` random bytes from the systems random source and
 * stores them into `ptr`.
 * @note We only use this we no TLS backend is available. Thus we can't piggy
 * back on the TLS backend's random source.
 */
static bool systemRandomBytes(void *ptr, size_t size)
{
#if defined(__BSD__) || defined(__APPLE__)
    arc4random_buf(ptr, size);
    return true;
#elif defined(__linux__) && \
    ((defined(__GLIBC__) && \
      (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 25))))
    return getentropy(ptr, size) != -1;
#elif defined(_WIN32)  // Windows
    return RtlGenRandom(ptr, (ULONG)size);
#elif defined(__unix__) || defined(__HAIKU__)
    // fallback to /dev/urandom for other/old UNIX
    thread_local std::unique_ptr<FILE, std::function<void(FILE *)> > fptr(
        fopen("/dev/urandom", "rb"), [](FILE *ptr) {
            if (ptr != nullptr)
                fclose(ptr);
        });
    if (fptr == nullptr)
    {
        LOG_FATAL << "Failed to open /dev/urandom for randomness";
        abort();
    }
    if (fread(ptr, 1, size, fptr.get()) != 0)
        return true;
#endif
    return false;
}
#endif

struct RngState
{
    Hash256 secret;
    Hash256 prev;
    int64_t time;
    uint64_t counter = 0;
};

bool secureRandomBytes(void *data, size_t len)
{
#if defined(USE_OPENSSL)
    // OpenSSL's RAND_bytes() uses int as the length parameter
    for (size_t i = 0; i < len; i += (std::numeric_limits<int>::max)())
    {
        int fillSize =
            (int)(std::min)(len - i, (size_t)(std::numeric_limits<int>::max)());
        if (!RAND_bytes((unsigned char *)data + i, fillSize))
            return false;
    }
    return true;
#elif defined(USE_BOTAN)
    thread_local Botan::AutoSeeded_RNG rng;
    rng.randomize((unsigned char *)data, len);
    return true;
#else
    // If no TLS backend is used, we use a CSPRNG of our own. This makes us use
    // up LESS system entropy. CSPRNG proposed by Dan Kaminsky in his DEFCON 22
    // talk.  With some modifications to make it suitable for trantor's
    // codebase. (RIP Dan Kaminsky. That talk was epic.)
    // https://youtu.be/xneBjc8z0DE?t=2250
    namespace chrono = std::chrono;
    static_assert(sizeof(RngState) < 128,
                  "RngState must be less then BLAKE2b's chunk size");

    thread_local int useCount = 0;
    thread_local RngState state;
    static const int64_t shiftAmount = []() {
        int64_t shift = 0;
        if (!systemRandomBytes(&shift, sizeof(shift)))
        {
            // fallback to a random device. Not guaranteed to be secure
            // but it's better than nothing.
            shift = std::random_device{}();
        }
        return shift;
    }();
    // Update secret every 1024 calls to this function
    if (useCount == 0)
    {
        if (!systemRandomBytes(&state.secret, sizeof(state.secret)))
            return false;
    }
    useCount = (useCount + 1) % 1024;

    // use the cycle counter register to get a bit more entropy.
    // Quote from the talk: "You can at least get a timestamp. And it turns out
    // you just needs bits that are different. .... If you integrate time. It
    // tuns out impericaly It's a pain in the butt to get two things to happen
    // at the exactly the same CPU nanosecond. It's not that it can't. IT'S THAT
    // IT WON'T. AND THAT'S A GOOD THING."
#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || \
    defined(_M_IX86)
    state.time = __rdtsc();
#elif defined(__aarch64__) || defined(_M_ARM64)
    // IMPORTANT! ARMv8 cntvct_el0 is not a cycle counter. It's a free running
    // counter that increments at 1~50MHz. 20~40x slower than the CPU. But
    // hashing takes more then that. So it's still good enough.
#ifdef _MSC_VER
    state.time = _ReadStatusReg(ARM64_CNTVCT_EL0);
#else
    asm volatile("mrs %0, cntvct_el0" : "=r"(state.time));
#endif
#elif defined(__riscv) && __riscv_xlen == 64
    asm volatile("rdtime %0" : "=r"(state.time));
#elif defined(__riscv) && __riscv_xlen == 32
    uint32_t timeLo, timeHi;
    asm volatile("rdtimeh %0" : "=r"(timeHi));
    asm volatile("rdtime %0" : "=r"(timeLo));
    state.time = (uint64_t)timeHi << 32 | timeLo;
#elif defined(__s390__)  // both s390 and s390x
    asm volatile("stck %0" : "=Q"(state.time));
#else
    auto now = chrono::steady_clock::now();
    // the proposed algorithm uses the time in nanoseconds, but we don't have a
    // way to read it (yet) not C++ provided a standard way to do it. Falling
    // back to milliseconds. This along with additional entropy is hopefully
    // good enough.
    state.time = chrono::time_point_cast<chrono::milliseconds>(now)
                     .time_since_epoch()
                     .count();
    // `now` lives on the stack, so address in each call _may_ be different.
    // This code works on both 32-bit and 64-bit systems. As well as big-endian
    // and little-endian systems.
    void *stack_ptr = &now;
    uint32_t *stack_ptr32 = (uint32_t *)&stack_ptr;
    uint32_t garbage = *stack_ptr32;
    static_assert(sizeof(void *) >= sizeof(uint32_t), "pointer size too small");
    for (size_t i = 1; i < sizeof(void *) / sizeof(uint32_t); i++)
        garbage ^= stack_ptr32[i];
    state.time ^= garbage;
#endif
    state.time += shiftAmount;

    // generate the random data as described in the talk. We use BLAKE2b since
    // it's fast and has a good security margin.
    for (size_t i = 0; i < len / sizeof(Hash256); i++)
    {
        auto hash = blake2b(&state, sizeof(state));
        memcpy((char *)data + i * sizeof(hash), &hash, sizeof(hash));
        state.counter++;
        state.prev = hash;
    }
    if (len % sizeof(Hash256) != 0)
    {
        auto hash = blake2b(&state, sizeof(state));
        memcpy((char *)data + len - len % sizeof(hash),
               &hash,
               len % sizeof(hash));
        state.counter++;
        state.prev = hash;
    }
    return true;
#endif
}

}  // namespace utils
}  // namespace trantor
