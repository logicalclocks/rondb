/**
 *
 *  Date.cc
 *  An Tao
 *
 *  Public header file in trantor lib.
 *
 *  Copyright 2018, An Tao.  All rights reserved.
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the License file.
 *
 *
 */

#include "Date.h"
#include "Funcs.h"
#include <cstdlib>
#include <iostream>
#include <string.h>
#ifdef _WIN32
#include <winsock2.h>
#include <time.h>
#else
#include <sys/time.h>
#endif

namespace trantor
{
#ifdef _WIN32
int gettimeofday(timeval *tp, void *tzp)
{
    time_t clock;
    struct tm tm;
    SYSTEMTIME wtm;

    GetLocalTime(&wtm);
    tm.tm_year = wtm.wYear - 1900;
    tm.tm_mon = wtm.wMonth - 1;
    tm.tm_mday = wtm.wDay;
    tm.tm_hour = wtm.wHour;
    tm.tm_min = wtm.wMinute;
    tm.tm_sec = wtm.wSecond;
    tm.tm_isdst = -1;
    clock = mktime(&tm);
    tp->tv_sec = static_cast<long>(clock);
    tp->tv_usec = wtm.wMilliseconds * 1000;

    return (0);
}
#endif

// tm_gmtoff is a BSD extension that glibc, musl, macOS and every BSD provide.
#ifdef _WIN32
// The MSVC CRT has no such member.
#elif defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__) || \
    defined(__OpenBSD__) || defined(__NetBSD__) || defined(__DragonFly__)
#define TRANTOR_HAS_TM_GMTOFF 1
#endif

namespace
{
// Thread-safe wrappers over the platform's time conversions.
inline bool localTimeOf(time_t seconds, struct tm &out)
{
    memset(&out, 0, sizeof(out));
#ifdef _WIN32
    // The MSVC signature is (tm *, const time_t *), the reverse of the C11
    // Annex K one, and it fails for instants outside [1970, 3000].
    return localtime_s(&out, &seconds) == 0;
#else
    return localtime_r(&seconds, &out) != nullptr;
#endif
}

inline bool utcTimeOf(time_t seconds, struct tm &out)
{
    memset(&out, 0, sizeof(out));
#ifdef _WIN32
    return gmtime_s(&out, &seconds) == 0;
#else
    return gmtime_r(&seconds, &out) != nullptr;
#endif
}

// Interpret the fields of tm as UTC; the inverse of utcTimeOf(). Available as
// timegm() on Linux/macOS/BSD and as _mkgmtime() on Windows.
inline time_t timeGm(struct tm &tm)
{
#ifdef _WIN32
    return _mkgmtime(&tm);
#else
    return timegm(&tm);
#endif
}

// The UTC offset (seconds, east of Greenwich positive) that the local time zone
// actually uses at the instant `seconds`. This is a function of the instant,
// not a constant: it changes with daylight saving time and with historical
// changes to a zone's standard offset.
int64_t utcOffsetAt(time_t seconds)
{
    struct tm localTm;
    if (!localTimeOf(seconds, localTm))
    {
        // The MSVC CRT rejects instants outside [1970-01-01, 3000-12-31].
        // Fall back to the epoch so we still report a plausible offset.
        seconds = 0;
        if (!localTimeOf(seconds, localTm))
            return 0;
    }
#ifdef TRANTOR_HAS_TM_GMTOFF
    return static_cast<int64_t>(localTm.tm_gmtoff);
#else
    // No tm_gmtoff: read the local wall clock back as if it were UTC; the
    // difference to the original instant is the offset. localTimeOf() has
    // already resolved DST, so nothing is guessed here.
    struct tm asUtc = localTm;
    const time_t reinterpreted = timeGm(asUtc);
    if (reinterpreted == static_cast<time_t>(-1))
        return 0;
    return static_cast<int64_t>(reinterpreted) - static_cast<int64_t>(seconds);
#endif
}

struct DateFields
{
    unsigned int year;
    unsigned int month;
    unsigned int day;
    unsigned int hour;
    unsigned int minute;
    unsigned int second;
    unsigned int microSecond;
};

// Build a Date from broken-down fields expressed in a time zone that is
// `utcOffsetSeconds` east of UTC (0 means the fields are plain UTC). Unlike the
// Date(year, month, ...) constructor this never consults the local time zone.
Date dateFromFields(const DateFields &f, int64_t utcOffsetSeconds)
{
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    tm.tm_year = static_cast<int>(f.year) - 1900;
    tm.tm_mon = static_cast<int>(f.month) - 1;
    tm.tm_mday = static_cast<int>(f.day);
    tm.tm_hour = static_cast<int>(f.hour);
    tm.tm_min = static_cast<int>(f.minute);
    tm.tm_sec = static_cast<int>(f.second);
    tm.tm_isdst = 0;  // unused by timegm()/_mkgmtime()
    const time_t epoch = timeGm(tm);
    return Date((static_cast<int64_t>(epoch) - utcOffsetSeconds) *
                    Date::MICRO_SECONDS_PER_SEC +
                f.microSecond);
}
}  // namespace

const Date Date::date()
{
#ifdef _WIN32
    timeval tv;
    gettimeofday(&tv, NULL);
    int64_t seconds = tv.tv_sec;
    return Date(seconds * MICRO_SECONDS_PER_SEC + tv.tv_usec);
#else
    struct timeval tv;
    gettimeofday(&tv, NULL);
    int64_t seconds = tv.tv_sec;
    return Date(seconds * MICRO_SECONDS_PER_SEC + tv.tv_usec);
#endif
}

int64_t Date::timezoneOffset(int64_t secondsSinceEpoch)
{
    return utcOffsetAt(static_cast<time_t>(secondsSinceEpoch));
}

int64_t Date::timezoneOffset()
{
    return timezoneOffset(Date::date().secondsSinceEpoch());
}

const Date Date::after(double second) const
{
    return Date(static_cast<int64_t>(microSecondsSinceEpoch_ +
                                     second * MICRO_SECONDS_PER_SEC));
}
const Date Date::roundSecond() const
{
    return Date(microSecondsSinceEpoch_ -
                (microSecondsSinceEpoch_ % MICRO_SECONDS_PER_SEC));
}
const Date Date::roundDay() const
{
    struct tm t;
    time_t seconds =
        static_cast<time_t>(microSecondsSinceEpoch_ / MICRO_SECONDS_PER_SEC);
#ifdef _WIN32
    localtime_s(&t, &seconds);
#else
    localtime_r(&seconds, &t);
#endif
    t.tm_hour = 0;
    t.tm_min = 0;
    t.tm_sec = 0;
    return Date(mktime(&t) * MICRO_SECONDS_PER_SEC);
}
struct tm Date::tmStruct() const
{
    time_t seconds =
        static_cast<time_t>(microSecondsSinceEpoch_ / MICRO_SECONDS_PER_SEC);
    struct tm tm_time;
#ifdef _WIN32
    gmtime_s(&tm_time, &seconds);
#else
    gmtime_r(&seconds, &tm_time);
#endif
    return tm_time;
}
std::string Date::toFormattedString(bool showMicroseconds) const
{
    //  std::cout<<"toFormattedString"<<std::endl;
    char buf[128] = {0};
    time_t seconds =
        static_cast<time_t>(microSecondsSinceEpoch_ / MICRO_SECONDS_PER_SEC);
    struct tm tm_time;
#ifdef _WIN32
    gmtime_s(&tm_time, &seconds);
#else
    gmtime_r(&seconds, &tm_time);
#endif

    if (showMicroseconds)
    {
        int microseconds =
            static_cast<int>(microSecondsSinceEpoch_ % MICRO_SECONDS_PER_SEC);
        snprintf(buf,
                 sizeof(buf),
                 "%4d%02d%02d %02d:%02d:%02d.%06d",
                 tm_time.tm_year + 1900,
                 tm_time.tm_mon + 1,
                 tm_time.tm_mday,
                 tm_time.tm_hour,
                 tm_time.tm_min,
                 tm_time.tm_sec,
                 microseconds);
    }
    else
    {
        snprintf(buf,
                 sizeof(buf),
                 "%4d%02d%02d %02d:%02d:%02d",
                 tm_time.tm_year + 1900,
                 tm_time.tm_mon + 1,
                 tm_time.tm_mday,
                 tm_time.tm_hour,
                 tm_time.tm_min,
                 tm_time.tm_sec);
    }
    return buf;
}
std::string Date::toCustomFormattedString(const std::string &fmtStr,
                                          bool showMicroseconds) const
{
    char buf[256] = {0};
    time_t seconds =
        static_cast<time_t>(microSecondsSinceEpoch_ / MICRO_SECONDS_PER_SEC);
    struct tm tm_time;
#ifdef _WIN32
    gmtime_s(&tm_time, &seconds);
#else
    gmtime_r(&seconds, &tm_time);
#endif
    strftime(buf, sizeof(buf), fmtStr.c_str(), &tm_time);
    if (!showMicroseconds)
        return std::string(buf);
    char decimals[12] = {0};
    int microseconds =
        static_cast<int>(microSecondsSinceEpoch_ % MICRO_SECONDS_PER_SEC);
    snprintf(decimals, sizeof(decimals), ".%06d", microseconds);
    return std::string(buf) + decimals;
}
void Date::toCustomFormattedString(const std::string &fmtStr,
                                   char *str,
                                   size_t len) const
{
    // not safe
    time_t seconds =
        static_cast<time_t>(microSecondsSinceEpoch_ / MICRO_SECONDS_PER_SEC);
    struct tm tm_time;
#ifdef _WIN32
    gmtime_s(&tm_time, &seconds);
#else
    gmtime_r(&seconds, &tm_time);
#endif
    strftime(str, len, fmtStr.c_str(), &tm_time);
}
std::string Date::toFormattedStringLocal(bool showMicroseconds) const
{
    //  std::cout<<"toFormattedString"<<std::endl;
    char buf[128] = {0};
    time_t seconds =
        static_cast<time_t>(microSecondsSinceEpoch_ / MICRO_SECONDS_PER_SEC);
    struct tm tm_time;
#ifdef _WIN32
    localtime_s(&tm_time, &seconds);
#else
    localtime_r(&seconds, &tm_time);
#endif

    if (showMicroseconds)
    {
        int microseconds =
            static_cast<int>(microSecondsSinceEpoch_ % MICRO_SECONDS_PER_SEC);
        snprintf(buf,
                 sizeof(buf),
                 "%4d%02d%02d %02d:%02d:%02d.%06d",
                 tm_time.tm_year + 1900,
                 tm_time.tm_mon + 1,
                 tm_time.tm_mday,
                 tm_time.tm_hour,
                 tm_time.tm_min,
                 tm_time.tm_sec,
                 microseconds);
    }
    else
    {
        snprintf(buf,
                 sizeof(buf),
                 "%4d%02d%02d %02d:%02d:%02d",
                 tm_time.tm_year + 1900,
                 tm_time.tm_mon + 1,
                 tm_time.tm_mday,
                 tm_time.tm_hour,
                 tm_time.tm_min,
                 tm_time.tm_sec);
    }
    return buf;
}
namespace
{
// Shared by toDbStringLocal() and toDbString(); the two differ only in which
// broken-down time they feed in.
std::string formatDbString(const struct tm &tm_time,
                           int64_t microSeconds,
                           bool dateOnly)
{
    char buf[128] = {0};
    if (microSeconds != 0)
    {
        snprintf(buf,
                 sizeof(buf),
                 "%4d-%02d-%02d %02d:%02d:%02d.%06d",
                 tm_time.tm_year + 1900,
                 tm_time.tm_mon + 1,
                 tm_time.tm_mday,
                 tm_time.tm_hour,
                 tm_time.tm_min,
                 tm_time.tm_sec,
                 static_cast<int>(microSeconds));
    }
    else if (dateOnly)
    {
        snprintf(buf,
                 sizeof(buf),
                 "%4d-%02d-%02d",
                 tm_time.tm_year + 1900,
                 tm_time.tm_mon + 1,
                 tm_time.tm_mday);
    }
    else
    {
        snprintf(buf,
                 sizeof(buf),
                 "%4d-%02d-%02d %02d:%02d:%02d",
                 tm_time.tm_year + 1900,
                 tm_time.tm_mon + 1,
                 tm_time.tm_mday,
                 tm_time.tm_hour,
                 tm_time.tm_min,
                 tm_time.tm_sec);
    }
    return buf;
}
}  // namespace

std::string Date::toDbStringLocal() const
{
    time_t seconds =
        static_cast<time_t>(microSecondsSinceEpoch_ / MICRO_SECONDS_PER_SEC);
    struct tm tm_time;
    localTimeOf(seconds, tm_time);
    return formatDbString(tm_time,
                          microSecondsSinceEpoch_ % MICRO_SECONDS_PER_SEC,
                          *this == roundDay());
}
std::string Date::toDbString() const
{
    // Format the instant in UTC directly. Shifting by a single "timezone
    // offset" and then formatting locally is wrong: the offset that applies
    // depends on the instant, so the local and the UTC representation of the
    // same Date do not share one.
    time_t seconds =
        static_cast<time_t>(microSecondsSinceEpoch_ / MICRO_SECONDS_PER_SEC);
    struct tm tm_time;
    utcTimeOf(seconds, tm_time);
    const bool dateOnly =
        (tm_time.tm_hour == 0 && tm_time.tm_min == 0 && tm_time.tm_sec == 0);
    return formatDbString(tm_time,
                          microSecondsSinceEpoch_ % MICRO_SECONDS_PER_SEC,
                          dateOnly);
}

// Parse a database datetime string into its calendar fields, without deciding
// which time zone those fields are expressed in.
static DateFields parseDbString(const std::string &datetime)
{
    DateFields f{};
    std::vector<std::string> &&v = splitString(datetime, " ");

    if (v.size() == 0)
    {
        throw std::invalid_argument("Invalid date string: " + datetime);
    }
    const std::vector<std::string> date = splitString(v[0], "-");
    if (date.size() != 3)
    {
        throw std::invalid_argument("Invalid date string: " + datetime);
    }
    if (v.size() == 1)
    {
        // Fromat YYYY-MM-DD is given
        try
        {
            f.year = std::stol(date[0]);
            f.month = std::stol(date[1]);
            f.day = std::stol(date[2]);
        }
        catch (...)
        {
            throw std::invalid_argument("Invalid date string: " + datetime);
        }
        return f;
    }

    if (v.size() == 2)
    {
        // Format YYYY-MM-DD HH:MM:SS[.UUUUUU] is given
        try
        {
            f.year = std::stol(date[0]);
            f.month = std::stol(date[1]);
            f.day = std::stol(date[2]);
            std::vector<std::string> time = splitString(v[1], ":");
            if (2 < time.size())
            {
                f.hour = std::stol(time[0]);
                f.minute = std::stol(time[1]);
                auto seconds = splitString(time[2], ".");
                f.second = std::stol(seconds[0]);
                if (1 < seconds.size())
                {
                    if (seconds[1].length() > 6)
                    {
                        seconds[1].resize(6);
                    }
                    else if (seconds[1].length() < 6)
                    {
                        seconds[1].append(6 - seconds[1].length(), '0');
                    }
                    f.microSecond = std::stol(seconds[1]);
                }
            }
        }
        catch (...)
        {
            throw std::invalid_argument("Invalid date string: " + datetime);
        }
        return f;
    }

    throw std::invalid_argument("Invalid date string: " + datetime);
}

Date Date::fromDbStringLocal(const std::string &datetime)
{
    const DateFields f = parseDbString(datetime);
    return Date(
        f.year, f.month, f.day, f.hour, f.minute, f.second, f.microSecond);
}

Date Date::fromDbString(const std::string &datetime)
{
    // Interpret the fields as UTC directly. Parsing them as local time and then
    // adding an offset cannot work: which offset applies depends on the instant
    // being parsed, and that is only known after the parse.
    return dateFromFields(parseDbString(datetime), 0);
}

static int parseTzOffset(std::string &tz, int tzSign)
{
    if (tz.empty())
    {
        return 0;
    }
    if (tzSign == 0)
    {
        if (tz[0] == '-' || tz[0] == '+')
        {
            tzSign = tz[0] == '-' ? -1 : 1;
            tz = tz.substr(1);
        }
        else
        {
            tzSign = 1;
        }
    }

    if (tz == "Z")
    {
        return 0;
    }

    auto tzParts = splitString(tz, ":");
    if (tzParts.size() == 1 && tz.size() >= 4)
    {
        tzParts = {tz.substr(0, 2), tz.substr(2)};  // 0800
    }

    int tzHour = tzParts.size() > 0 ? std::stoi(tzParts[0]) : 0;
    int tzMin = tzParts.size() > 1 ? std::stoi(tzParts[1]) : 0;
    return tzSign * (tzHour * 3600 + tzMin * 60);
}

// Variant of splitString(), accepts multiple single-char delimiters
static std::vector<std::string> splitString2(const std::string &s,
                                             const std::string &delimiters,
                                             bool acceptEmptyString = false)
{
    if (delimiters.empty())
        return std::vector<std::string>{};
    std::vector<std::string> v;
    size_t last = 0;
    size_t next = 0;
    while ((next = s.find_first_of(delimiters, last)) != std::string::npos)
    {
        if (next > last || acceptEmptyString)
            v.push_back(s.substr(last, next - last));
        last = next + 1;
    }
    if (s.length() > last || acceptEmptyString)
        v.push_back(s.substr(last));
    return v;
}

Date Date::fromISOString(const std::string &datetime)
{
    unsigned int year = {0}, month = {0}, day = {0}, hour = {0}, minute = {0},
                 second = {0}, microSecond = {0};
    int tzSign{0}, tzOffset{0};
    std::vector<std::string> v = splitString2(datetime, "T ");
    if (v.empty())
    {
        throw std::invalid_argument("Invalid date string: " + datetime);
    }

    // parse date
    const std::vector<std::string> date = splitString(v[0], "-");
    if (date.size() != 3)
    {
        throw std::invalid_argument("Invalid date string: " + datetime);
    }
    year = std::stol(date[0]);
    month = std::stol(date[1]);
    day = std::stol(date[2]);

    // only have date part
    if (v.size() <= 1)
    {
        return trantor::Date{year, month, day};
    }

    // check timezone without space separated
    if (v.size() == 2)
    {
        auto pos = v[1].find('+');
        if (pos != std::string::npos)
        {
            tzSign = 1;
            v.push_back(v[1].substr(pos + 1));
            v[1] = v[1].substr(0, pos);
        }
        else if ((pos = v[1].find('-')) != std::string::npos)
        {
            tzSign = -1;
            v.push_back(v[1].substr(pos + 1));
            v[1] = v[1].substr(0, pos);
        }
        else if (!v[1].empty() && v[1].back() == 'Z')
        {
            v[1].pop_back();
            tzSign = 1;
            v.emplace_back("Z");
        }
    }

    // parse time
    std::vector<std::string> timeParts = splitString(v[1], ":");
    if (timeParts.size() < 2 || timeParts.size() > 3)
    {
        throw std::invalid_argument("Invalid time string: " + datetime);
    }
    hour = std::stol(timeParts[0]);
    minute = std::stol(timeParts[1]);
    if (timeParts.size() == 3)
    {
        auto secParts = splitString(timeParts[2], ".");
        second = std::stol(secParts[0]);
        // micro seconds
        if (secParts.size() > 1)
        {
            if (secParts[1].length() > 6)
            {
                secParts[1].resize(6);
            }
            else if (secParts[1].length() < 6)
            {
                secParts[1].append(6 - secParts[1].length(), '0');
            }
            microSecond = std::stol(secParts[1]);
        }
    }

    // timezone
    if (v.size() >= 3)
    {
        // The string carries its own UTC offset, so the local time zone must
        // not take part in the conversion at all.
        tzOffset = parseTzOffset(v[2], tzSign);
        const DateFields fields{
            year, month, day, hour, minute, second, microSecond};
        return dateFromFields(fields, tzOffset);
    }
    // No time zone in the string: keep interpreting it as local time.
    return trantor::Date(year, month, day, hour, minute, second, microSecond);
}

std::string Date::toCustomFormattedStringLocal(const std::string &fmtStr,
                                               bool showMicroseconds) const
{
    char buf[256] = {0};
    time_t seconds =
        static_cast<time_t>(microSecondsSinceEpoch_ / MICRO_SECONDS_PER_SEC);
    struct tm tm_time;
#ifdef _WIN32
    localtime_s(&tm_time, &seconds);
#else
    localtime_r(&seconds, &tm_time);
#endif
    strftime(buf, sizeof(buf), fmtStr.c_str(), &tm_time);
    if (!showMicroseconds)
        return std::string(buf);
    char decimals[12] = {0};
    int microseconds =
        static_cast<int>(microSecondsSinceEpoch_ % MICRO_SECONDS_PER_SEC);
    snprintf(decimals, sizeof(decimals), ".%06d", microseconds);
    return std::string(buf) + decimals;
}
Date::Date(unsigned int year,
           unsigned int month,
           unsigned int day,
           unsigned int hour,
           unsigned int minute,
           unsigned int second,
           unsigned int microSecond)
{
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    tm.tm_isdst = -1;
    time_t epoch;
    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;
    tm.tm_hour = hour;
    tm.tm_min = minute;
    tm.tm_sec = second;
    epoch = mktime(&tm);
    microSecondsSinceEpoch_ =
        static_cast<int64_t>(epoch) * MICRO_SECONDS_PER_SEC + microSecond;
}

}  // namespace trantor
