#include <trantor/utils/Date.h>
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <iostream>
#ifndef _WIN32
#include <cstdlib>
#include <ctime>
#endif
using namespace trantor;
TEST(Date, constructorTest)
{
    EXPECT_STREQ("1985-01-01 00:00:00",
                 trantor::Date(1985, 1, 1)
                     .toCustomFormattedStringLocal("%Y-%m-%d %H:%M:%S")
                     .c_str());
    EXPECT_STREQ("2004-02-29 00:00:00.000000",
                 trantor::Date(2004, 2, 29)
                     .toCustomFormattedStringLocal("%Y-%m-%d %H:%M:%S", true)
                     .c_str());
    EXPECT_STRNE("2001-02-29 00:00:00.000000",
                 trantor::Date(2001, 2, 29)
                     .toCustomFormattedStringLocal("%Y-%m-%d %H:%M:%S", true)
                     .c_str());
    EXPECT_STREQ("2018-01-01 00:00:00.000000",
                 trantor::Date(2018, 1, 1, 12, 12, 12, 2321)
                     .roundDay()
                     .toCustomFormattedStringLocal("%Y-%m-%d %H:%M:%S", true)
                     .c_str());
}
TEST(Date, DatabaseStringTest)
{
    auto now = trantor::Date::now();
    EXPECT_EQ(now, trantor::Date::fromDbStringLocal(now.toDbStringLocal()));
    EXPECT_EQ(now, trantor::Date::fromDbString(now.toDbString()));
    std::string dbString = "2018-01-01 00:00:00.123";
    auto dbDate = trantor::Date::fromDbStringLocal(dbString);
    auto ms = (dbDate.microSecondsSinceEpoch() % 1000000) / 1000;
    EXPECT_EQ(ms, 123);
    EXPECT_EQ(dbDate,
              trantor::Date::fromDbStringLocal(dbDate.toDbStringLocal()));
    EXPECT_EQ(dbDate, trantor::Date::fromDbString(dbDate.toDbString()));
    dbString = "2018-01-01 00:00:00.023";
    dbDate = trantor::Date::fromDbStringLocal(dbString);
    ms = (dbDate.microSecondsSinceEpoch() % 1000000) / 1000;
    EXPECT_EQ(ms, 23);
    EXPECT_EQ(dbDate,
              trantor::Date::fromDbStringLocal(dbDate.toDbStringLocal()));
    EXPECT_EQ(dbDate, trantor::Date::fromDbString(dbDate.toDbString()));
    dbString = "2018-01-01 00:00:00.003";
    dbDate = trantor::Date::fromDbStringLocal(dbString);
    ms = (dbDate.microSecondsSinceEpoch() % 1000000) / 1000;
    EXPECT_EQ(ms, 3);
    EXPECT_EQ(dbDate,
              trantor::Date::fromDbStringLocal(dbDate.toDbStringLocal()));
    EXPECT_EQ(dbDate, trantor::Date::fromDbString(dbDate.toDbString()));
    dbString = "2018-01-01 00:00:00.000123";
    dbDate = trantor::Date::fromDbStringLocal(dbString);
    auto us = (dbDate.microSecondsSinceEpoch() % 1000000);
    EXPECT_EQ(us, 123);
    EXPECT_EQ(dbDate,
              trantor::Date::fromDbStringLocal(dbDate.toDbStringLocal()));
    EXPECT_EQ(dbDate, trantor::Date::fromDbString(dbDate.toDbString()));
    dbString = "2018-01-01 00:00:00.000023";
    dbDate = trantor::Date::fromDbStringLocal(dbString);
    us = (dbDate.microSecondsSinceEpoch() % 1000000);
    EXPECT_EQ(us, 23);
    EXPECT_EQ(dbDate,
              trantor::Date::fromDbStringLocal(dbDate.toDbStringLocal()));
    EXPECT_EQ(dbDate, trantor::Date::fromDbString(dbDate.toDbString()));
    dbString = "2018-01-01 00:00:00.000003";
    dbDate = trantor::Date::fromDbStringLocal(dbString);
    us = (dbDate.microSecondsSinceEpoch() % 1000000);
    EXPECT_EQ(us, 3);

    dbString = "2018-01-01 00:00:00";
    dbDate = trantor::Date::fromDbStringLocal(dbString);
    ms = (dbDate.microSecondsSinceEpoch() % 1000000) / 1000;
    EXPECT_EQ(ms, 0);

    dbString = "2018-01-01 00:00:00";
    dbDate = trantor::Date::fromDbStringLocal(dbString);
    auto dbDateGMT = trantor::Date::fromDbString(dbString);
    auto secLocal = (dbDate.microSecondsSinceEpoch() / 1000000);
    auto secGMT = (dbDateGMT.microSecondsSinceEpoch() / 1000000);
    // timeZone at least 1 minute (can be >=1 hour, 30 min, 15 min. Error if
    // difference less then minute)
    auto timeZoneOffsetMinutePart = (secLocal - secGMT) % 60;
    EXPECT_EQ(timeZoneOffsetMinutePart, 0);
    dbString = "2018-01-01 00:00:00.123";
    dbDate = trantor::Date::fromDbString(dbString);
    ms = (dbDate.microSecondsSinceEpoch() % 1000000) / 1000;
    EXPECT_EQ(ms, 123);
    dbString = "2018-01-01 00:00:00.023";
    dbDate = trantor::Date::fromDbString(dbString);
    ms = (dbDate.microSecondsSinceEpoch() % 1000000) / 1000;
    EXPECT_EQ(ms, 23);
    dbString = "2018-01-01 00:00:00.003";
    dbDate = trantor::Date::fromDbString(dbString);
    ms = (dbDate.microSecondsSinceEpoch() % 1000000) / 1000;
    EXPECT_EQ(ms, 3);
    dbString = "2018-01-01 00:00:00.000123";
    dbDate = trantor::Date::fromDbString(dbString);
    us = (dbDate.microSecondsSinceEpoch() % 1000000);
    EXPECT_EQ(us, 123);
    dbString = "2018-01-01 00:00:00.000023";
    dbDate = trantor::Date::fromDbString(dbString);
    us = (dbDate.microSecondsSinceEpoch() % 1000000);
    EXPECT_EQ(us, 23);
    dbString = "2018-01-01 00:00:00.000003";
    dbDate = trantor::Date::fromDbString(dbString);
    us = (dbDate.microSecondsSinceEpoch() % 1000000);
    EXPECT_EQ(us, 3);

    dbString = "1970-01-01";
    dbDateGMT = trantor::Date::fromDbString(dbString);
    auto epoch = dbDateGMT.microSecondsSinceEpoch();
    EXPECT_EQ(epoch, 0);
}
TEST(Date, TimezoneTest)
{
    std::string dat0 = "2024-01-01";
    std::string str0 = "2024-01-01 04:00:00.123";
    std::vector<std::string> strs{
        // in case we miss any comma, put brackets around
        {"2024-01-01 04:00:00.123Z"},
        {"2024-01-01 12:00:00.123 +08:00"},
        {"2024-01-01 11:00:00.123+0700"},
        {"2024-01-01 10:00:00.123 0600"},
        {"2024-01-01 09:00:00.123 +0500"},
        {"2024-01-01 08:00:00.123 04"},
        {"2024-01-01 07:00:00.123+03"},
        {"2024-01-01 06:30:00.123+02:30"},
        {"2024-01-01 03:00:00.123 -01:00"},
        {"2024-01-01 02:00:00.123-02:00"},
        {"2024-01-01 01:00:00.123 -0300"},
        {"2024-01-01 00:00:00.123-04"},
        {"2023-12-31 23:00:00.123 -05"},
        // with T
        {"2024-01-01T04:00:00.123000Z"},
        {"2024-01-01T12:00:00.123 +08:00"},
        // bad ones, but should pass
        {"2024-01-01T04:00:00.123+0"},
        {"2024-01-01T04:00:00.123-"},
    };

    auto date = trantor::Date::fromDbString(str0);
    for (auto &s : strs)
    {
        auto dateTz = trantor::Date::fromISOString(s);
        EXPECT_EQ(date.microSecondsSinceEpoch(),
                  dateTz.microSecondsSinceEpoch());
    }

    // time string without tz, should be parsed as local time
    auto dateLocal = trantor::Date::fromDbStringLocal(str0);
    EXPECT_EQ(dateLocal.microSecondsSinceEpoch(),
              trantor::Date::fromISOString(str0).microSecondsSinceEpoch());

    // only date part
    EXPECT_EQ(dateLocal.secondsSinceEpoch() - 4 * 3600,
              trantor::Date::fromISOString(dat0).secondsSinceEpoch());
}

#ifndef _WIN32
// The MSVC CRT only understands the legacy "tzn[+|-]hh[:mm[:ss]][dzn]" form of
// TZ and always applies the US daylight-saving rules, so IANA zone names -- and
// therefore these tests -- are POSIX-only.
namespace
{
class TzGuard
{
  public:
    explicit TzGuard(const char *tz)
    {
        const char *previous = getenv("TZ");
        had_ = (previous != nullptr);
        if (had_)
            old_ = previous;
        setenv("TZ", tz, 1);
        // localtime_r() is not required to call tzset() implicitly.
        tzset();
    }
    ~TzGuard()
    {
        if (had_)
            setenv("TZ", old_.c_str(), 1);
        else
            unsetenv("TZ");
        tzset();
    }
    TzGuard(const TzGuard &) = delete;
    TzGuard &operator=(const TzGuard &) = delete;

  private:
    std::string old_;
    bool had_{false};
};

// Precondition check that does not go through trantor: if the zoneinfo
// database is missing, the C library silently falls back to UTC and the
// expectations below would be meaningless.
bool zoneLoaded(time_t instant, long expectedOffset)
{
    struct tm tmv;
    if (localtime_r(&instant, &tmv) == nullptr)
        return false;
    return tmv.tm_gmtoff == expectedOffset;
}

constexpr time_t kWinter = 1767225600;  // 2026-01-01T00:00:00Z
constexpr time_t kSummer = 1782864000;  // 2026-07-01T00:00:00Z
}  // namespace

// Regression test for https://github.com/an-tao/trantor/issues/401: the offset
// must be the one in effect at the given instant, not a constant sampled at
// 1970-01-01.
TEST(Date, timezoneOffsetFollowsDst)
{
    TzGuard tz("Europe/Paris");
    if (!zoneLoaded(kWinter, 3600))
        GTEST_SKIP() << "zoneinfo for Europe/Paris is not available";

    EXPECT_EQ(trantor::Date::timezoneOffset(kWinter), 3600);  // CET
    EXPECT_EQ(trantor::Date::timezoneOffset(kSummer), 7200);  // CEST
}

// A zone whose standard offset itself changed after 1970 must not be pinned to
// its 1970 value either.
TEST(Date, timezoneOffsetFollowsHistoricalChanges)
{
    TzGuard tz("Europe/Lisbon");
    if (!zoneLoaded(kWinter, 0))
        GTEST_SKIP() << "zoneinfo for Europe/Lisbon is not available";

    EXPECT_EQ(trantor::Date::timezoneOffset(0), 3600);        // CET in 1970
    EXPECT_EQ(trantor::Date::timezoneOffset(kWinter), 0);     // WET today
    EXPECT_EQ(trantor::Date::timezoneOffset(kSummer), 3600);  // WEST today
}

TEST(Date, timezoneOffsetSouthernHemisphere)
{
    TzGuard tz("America/Santiago");
    if (!zoneLoaded(kWinter, -10800))
        GTEST_SKIP() << "zoneinfo for America/Santiago is not available";

    // January is summer time here, July is not.
    EXPECT_EQ(trantor::Date::timezoneOffset(kWinter), -10800);
    EXPECT_EQ(trantor::Date::timezoneOffset(kSummer), -14400);
}

TEST(Date, toDbStringIsUtcAcrossDst)
{
    TzGuard tz("Europe/Paris");
    if (!zoneLoaded(kWinter, 3600))
        GTEST_SKIP() << "zoneinfo for Europe/Paris is not available";

    const trantor::Date summer(static_cast<int64_t>(kSummer) *
                               trantor::Date::MICRO_SECONDS_PER_SEC);
    EXPECT_EQ(summer.toDbString(), "2026-07-01");
    EXPECT_EQ(summer.toDbStringLocal(), "2026-07-01 02:00:00");

    const trantor::Date winter(static_cast<int64_t>(kWinter) *
                               trantor::Date::MICRO_SECONDS_PER_SEC);
    EXPECT_EQ(winter.toDbString(), "2026-01-01");
    EXPECT_EQ(winter.toDbStringLocal(), "2026-01-01 01:00:00");
}

TEST(Date, dbStringRoundTripAcrossTimezones)
{
    const char *zones[] = {"Europe/Paris",
                           "America/New_York",
                           "Asia/Tokyo",
                           "America/Santiago",
                           "Australia/Sydney",
                           "Asia/Kathmandu",
                           "UTC"};
    const int64_t instants[] = {0, kWinter, kSummer, 1234567890, 2000000000};
    for (const char *zone : zones)
    {
        TzGuard tz(zone);
        for (int64_t seconds : instants)
        {
            const trantor::Date d(seconds *
                                  trantor::Date::MICRO_SECONDS_PER_SEC);
            EXPECT_EQ(trantor::Date::fromDbString(d.toDbString()), d)
                << zone << " @" << seconds;
            EXPECT_EQ(trantor::Date::fromDbStringLocal(d.toDbStringLocal()), d)
                << zone << " @" << seconds;
        }
    }
}

// An ISO string that carries its own offset must parse to the same instant no
// matter what the local time zone is.
TEST(Date, fromISOStringIgnoresLocalTimezone)
{
    const char *zones[] = {"Europe/Paris",
                           "America/New_York",
                           "Asia/Tokyo",
                           "Australia/Sydney",
                           "UTC"};
    for (const char *zone : zones)
    {
        TzGuard tz(zone);
        EXPECT_EQ(trantor::Date::fromISOString("2026-07-01T00:00:00Z")
                      .secondsSinceEpoch(),
                  kSummer)
            << zone;
        EXPECT_EQ(trantor::Date::fromISOString("2026-07-01T02:00:00+02:00")
                      .secondsSinceEpoch(),
                  kSummer)
            << zone;
        EXPECT_EQ(trantor::Date::fromISOString("2025-12-31T19:00:00-0500")
                      .secondsSinceEpoch(),
                  kWinter)
            << zone;
    }
}
#endif  // !_WIN32

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
