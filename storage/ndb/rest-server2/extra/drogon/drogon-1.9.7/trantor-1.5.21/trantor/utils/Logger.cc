/**
 *
 *  Logger.cc
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

#include <trantor/utils/Logger.h>
#include <stdio.h>
#include <thread>
#ifdef __unix__
#include <unistd.h>
#include <sys/syscall.h>
#include <sstream>
#elif defined __HAIKU__
#include <sstream>
#include <unistd.h>
#elif defined _WIN32
#include <sstream>
#endif
#ifdef TRANTOR_SPDLOG_SUPPORT
#include <spdlog/spdlog.h>
#include <map>
#include <mutex>
#endif
#if (__cplusplus >= 201703L) || \
    (defined(_MSVC_LANG) &&     \
     (_MSVC_LANG >=             \
      201703L))  // c++17 - the _MSVC_LANG extra check can be
                 // removed if the support for VS2015 & 2017 is dropped
#include <algorithm>
#else
namespace std
{
inline trantor::Logger::LogLevel clamp(trantor::Logger::LogLevel v,
                                       trantor::Logger::LogLevel min,
                                       trantor::Logger::LogLevel max)
{
    return (v < min) ? min : (v > max) ? max : v;
}
}  // namespace std
#endif
#if defined __FreeBSD__
#include <pthread_np.h>
#endif

namespace trantor
{
// helper class for known string length at compile time
class T
{
  public:
    T(const char *str, unsigned len) : str_(str), len_(len)
    {
        assert(strlen(str) == len_);
    }

    const char *str_;
    const unsigned len_;
};

const char *strerror_tl(int savedErrno)
{
#ifndef _MSC_VER
    return strerror(savedErrno);
#else
    static thread_local char errMsg[64];
    (void)strerror_s<sizeof errMsg>(errMsg, savedErrno);
    return errMsg;
#endif
}

inline LogStream &operator<<(LogStream &s, T v)
{
    s.append(v.str_, v.len_);
    return s;
}

inline LogStream &operator<<(LogStream &s, const Logger::SourceFile &v)
{
    s.append(v.data_, v.size_);
    return s;
}

}  // namespace trantor
using namespace trantor;

static thread_local uint64_t lastSecond_{0};
static thread_local char lastTimeString_[32] = {0};
#ifdef __linux__
static thread_local pid_t threadId_{0};
#else
static thread_local uint64_t threadId_{0};
#endif
//   static thread_local LogStream logStream_;

void Logger::formatTime()
{
    uint64_t now = static_cast<uint64_t>(date_.secondsSinceEpoch());
    uint64_t microSec =
        static_cast<uint64_t>(date_.microSecondsSinceEpoch() -
                              date_.roundSecond().microSecondsSinceEpoch());
    if (now != lastSecond_)
    {
        lastSecond_ = now;
        if (displayLocalTime_())
        {
#ifndef _MSC_VER
            strncpy(lastTimeString_,
                    date_.toFormattedStringLocal(false).c_str(),
                    sizeof(lastTimeString_) - 1);
#else
            strncpy_s<sizeof lastTimeString_>(
                lastTimeString_,
                date_.toFormattedStringLocal(false).c_str(),
                sizeof(lastTimeString_) - 1);
#endif
        }
        else
        {
#ifndef _MSC_VER
            strncpy(lastTimeString_,
                    date_.toFormattedString(false).c_str(),
                    sizeof(lastTimeString_) - 1);
#else
            strncpy_s<sizeof lastTimeString_>(
                lastTimeString_,
                date_.toFormattedString(false).c_str(),
                sizeof(lastTimeString_) - 1);
#endif
        }
    }
    logStream_ << T(lastTimeString_, 17);
    char tmp[32];
    if (displayLocalTime_())
    {
        snprintf(tmp,
                 sizeof(tmp),
                 ".%06llu ",
                 static_cast<long long unsigned int>(microSec));
        logStream_ << T(tmp, 8);
    }
    else
    {
        snprintf(tmp,
                 sizeof(tmp),
                 ".%06llu UTC ",
                 static_cast<long long unsigned int>(microSec));
        logStream_ << T(tmp, 12);
    }
#ifdef __linux__
    if (threadId_ == 0)
        threadId_ = static_cast<pid_t>(::syscall(SYS_gettid));
#elif defined __FreeBSD__
    if (threadId_ == 0)
    {
        threadId_ = pthread_getthreadid_np();
    }
#elif defined __OpenBSD__
    if (threadId_ == 0)
    {
        threadId_ = getthrid();
    }
#elif defined __sun
    if (threadId_ == 0)
    {
        threadId_ = static_cast<uint64_t>(pthread_self());
    }
#elif defined _WIN32 || defined __HAIKU__
    if (threadId_ == 0)
    {
        std::stringstream ss;
        ss << std::this_thread::get_id();
        threadId_ = std::stoull(ss.str());
    }
#else
    if (threadId_ == 0)
    {
        pthread_threadid_np(NULL, &threadId_);
    }
#endif
    logStream_ << threadId_;
}
static const char *logLevelStr[Logger::LogLevel::kNumberOfLogLevels] = {
    " TRACE ",
    " DEBUG ",
    " INFO  ",
    " WARN  ",
    " ERROR ",
    " FATAL ",
};

Logger::Logger(SourceFile file, int line)
    : sourceFile_(file), fileLine_(line), level_(kInfo)
{
    formatTime();
    logStream_ << T(logLevelStr[level_], 7);
#ifdef TRANTOR_SPDLOG_SUPPORT
    spdLogMessageOffset_ = logStream_.bufferLength();
#endif
}
Logger::Logger(SourceFile file, int line, LogLevel level)
    : sourceFile_(file),
      fileLine_(line),
      level_(std::clamp(level, kTrace, kFatal))
{
    formatTime();
    logStream_ << T(logLevelStr[level_], 7);
#ifdef TRANTOR_SPDLOG_SUPPORT
    spdLogMessageOffset_ = logStream_.bufferLength();
#endif
}
Logger::Logger(SourceFile file, int line, LogLevel level, const char *func)
    : sourceFile_(file),
      fileLine_(line),
      level_(std::clamp(level, kTrace, kFatal))
#ifdef TRANTOR_SPDLOG_SUPPORT
      ,
      func_(func)
#endif
{
    formatTime();
    logStream_ << T(logLevelStr[level_], 7) << "[" << func << "] ";
#ifdef TRANTOR_SPDLOG_SUPPORT
    spdLogMessageOffset_ = logStream_.bufferLength();
#endif
}
Logger::Logger(SourceFile file, int line, bool)
    : sourceFile_(file), fileLine_(line), level_(kFatal)
{
    formatTime();
    logStream_ << T(logLevelStr[level_], 7);
#ifdef TRANTOR_SPDLOG_SUPPORT
    spdLogMessageOffset_ = logStream_.bufferLength();
#endif
    if (errno != 0)
    {
        logStream_ << strerror_tl(errno) << " (errno=" << errno << ") ";
    }
}

// LOG_COMPACT
Logger::Logger() : level_(kInfo)
{
    formatTime();
    logStream_ << T(logLevelStr[level_], 7);
#ifdef TRANTOR_SPDLOG_SUPPORT
    spdLogMessageOffset_ = logStream_.bufferLength();
#endif
}
Logger::Logger(LogLevel level) : level_(std::clamp(level, kTrace, kFatal))
{
    formatTime();
    logStream_ << T(logLevelStr[level_], 7);
#ifdef TRANTOR_SPDLOG_SUPPORT
    spdLogMessageOffset_ = logStream_.bufferLength();
#endif
}
Logger::Logger(bool) : level_(kFatal)
{
    formatTime();
    logStream_ << T(logLevelStr[level_], 7);
#ifdef TRANTOR_SPDLOG_SUPPORT
    spdLogMessageOffset_ = logStream_.bufferLength();
#endif
    if (errno != 0)
    {
        logStream_ << strerror_tl(errno) << " (errno=" << errno << ") ";
    }
}

bool Logger::hasSpdLogSupport()
{
#ifdef TRANTOR_SPDLOG_SUPPORT
    return true;
#else
    return false;
#endif
}

#ifdef TRANTOR_SPDLOG_SUPPORT
// Helper for uniform naming
static std::string defaultSpdLoggerName(int index)
{
    using namespace std::literals::string_literals;
    std::string loggerName = "trantor"s;
    if (index >= 0)
        loggerName.append(std::to_string(index));
    return loggerName;
}
// a map with int keys is more efficient than spdlog internal registry based on
// strings (logger name)
static std::map<int, std::shared_ptr<spdlog::logger>> spdLoggers;
// same sinks, but the format pattern is only "%v", for LOG_RAW[_TO]
static std::map<int, std::shared_ptr<spdlog::logger>> rawSpdLoggers;
static std::mutex spdLoggersMtx;
#endif  // TRANTOR_SPDLOG_SUPPORT

std::shared_ptr<spdlog::logger> Logger::getDefaultSpdLogger(int index)
{
#ifdef TRANTOR_SPDLOG_SUPPORT
    auto loggerName = defaultSpdLoggerName(index);
    auto logger = spdlog::get(loggerName);
    if (logger)
        return logger;
    // Create a new spdlog logger with the same sinks as the current default
    // Logger or spdlog logger
    auto &sinks =
        ((spdLoggers.begin() != spdLoggers.end() ? spdLoggers.begin()->second
                                                 : spdlog::default_logger()))
            ->sinks();
    logger = std::make_shared<spdlog::logger>(loggerName,
                                              sinks.begin(),
                                              sinks.end());
    // keep a log format similar to the existing one, but with coloured
    // level on console since it's nice :)
    // see reference: https://github.com/gabime/spdlog/wiki/3.-Custom-formatting
    logger->set_pattern("%Y%m%d %T.%f %6t %^%=8l%$ [%!] %v - %s:%#");
    // the filtering is done at Logger level, so no need to filter here
    logger->set_level(spdlog::level::trace);
    logger->flush_on(spdlog::level::err);
    spdlog::register_logger(logger);

    return logger;
#else
    (void)index;
    return {};
#endif  // TRANTOR_SPDLOG_SUPPORT
}

std::shared_ptr<spdlog::logger> Logger::getSpdLogger(int index)
{
#ifdef TRANTOR_SPDLOG_SUPPORT
    std::lock_guard<std::mutex> lck(spdLoggersMtx);
    auto it = spdLoggers.find((index < 0) ? -1 : index);
    return (it == spdLoggers.end()) ? std::shared_ptr<spdlog::logger>()
                                    : it->second;
#else
    (void)index;
    return {};
#endif  // TRANTOR_SPDLOG_SUPPORT
}

#ifdef TRANTOR_SPDLOG_SUPPORT
static std::shared_ptr<spdlog::logger> getRawSpdLogger(int index)
{
    // Create/delete RAW logger on-the fly
    // drawback: changes to the main logger's level or sinks won't be
    //           reflected in the raw logger once it's created
    if (index < -1)
        index = -1;
    std::lock_guard<std::mutex> lck(spdLoggersMtx);
    auto itMain = spdLoggers.find(index);
    auto itRaw = rawSpdLoggers.find(index);
    if (itMain == spdLoggers.end())
    {
        if (itRaw != rawSpdLoggers.end())
        {
            spdlog::drop(itRaw->second->name());
            rawSpdLoggers.erase(itRaw);
        }
        return {};
    }
    auto mainLogger = itMain->second;
    if (itRaw != rawSpdLoggers.end())
        return itRaw->second;
    auto rawLogger =
        std::make_shared<spdlog::logger>(mainLogger->name() + "_raw",
                                         mainLogger->sinks().begin(),
                                         mainLogger->sinks().end());
    rawLogger->set_pattern("%v");
    rawLogger->set_level(mainLogger->level());
    rawLogger->flush_on(mainLogger->flush_level());
    rawSpdLoggers[index] = rawLogger;
    spdlog::register_logger(rawLogger);
    return rawLogger;
}
#endif  // TRANTOR_SPDLOG_SUPPORT

void Logger::enableSpdLog(int index, std::shared_ptr<spdlog::logger> logger)
{
#ifdef TRANTOR_SPDLOG_SUPPORT
    if (index < -1)
        index = -1;
    std::lock_guard<std::mutex> lck(spdLoggersMtx);
    spdLoggers[index] = logger ? logger : getDefaultSpdLogger(index);
#else
    (void)index;
    (void)logger;
#endif  // TRANTOR_SPDLOG_SUPPORT
}

void Logger::disableSpdLog(int index)
{
#ifdef TRANTOR_SPDLOG_SUPPORT
    std::lock_guard<std::mutex> lck(spdLoggersMtx);
    if (index < -1)
        index = -1;
    auto it = spdLoggers.find(index);
    if (it == spdLoggers.end())
        return;
    // auto-unregister
    if (it->second->name() == defaultSpdLoggerName(index))
        spdlog::drop(it->second->name());
    spdLoggers.erase(it);
#else
    (void)index;
#endif  // TRANTOR_SPDLOG_SUPPORT
}

RawLogger::~RawLogger()
{
#ifdef TRANTOR_SPDLOG_SUPPORT
    auto logger = getRawSpdLogger(index_);
    if (logger)
    {
        // The only way to be fully compatible with the existing non-spdlog RAW
        // mode (dumping raw without adding a '\n') would be to store the
        // concatenated messages along the logger, and pass the complete message
        // to spdlog only when it ends with '\n'.
        // But it's overkill...
        // For now, just remove the trailing '\n', if any, since spdlog
        // automatically adds one.
        auto msglen = logStream_.bufferLength();
        if ((msglen > 0) && (logStream_.bufferData()[msglen - 1] == '\n'))
            msglen--;
        logger->info(spdlog::string_view_t(logStream_.bufferData(), msglen));
        return;
    }
#endif
    if (index_ < 0)
    {
        auto &oFunc = Logger::outputFunc_();
        if (!oFunc)
            return;
        oFunc(logStream_.bufferData(), logStream_.bufferLength());
    }
    else
    {
        auto &oFunc = Logger::outputFunc_(index_);
        if (!oFunc)
            return;
        oFunc(logStream_.bufferData(), logStream_.bufferLength());
    }
}

Logger::~Logger()
{
#ifdef TRANTOR_SPDLOG_SUPPORT
    auto spdLogger = getSpdLogger(index_);
    if (spdLogger)
    {
        spdlog::source_loc spdLocation;
        if (sourceFile_.data_)
            spdLocation = {sourceFile_.data_, fileLine_, func_ ? func_ : ""};
        spdlog::string_view_t message(logStream_.bufferData(),
                                      logStream_.bufferLength());
        message.remove_prefix(spdLogMessageOffset_);
#if defined(SPDLOG_VERSION) && (SPDLOG_VERSION >= 10600)
        spdLogger->log(std::chrono::system_clock::time_point(
                           std::chrono::duration<int64_t, std::micro>(
                               date_.microSecondsSinceEpoch())),
                       spdLocation,
                       spdlog::level::level_enum(level_),
                       message);
#else  // very old version, cannot specify time
        spdLogger->log(spdLocation, spdlog::level::level_enum(level_), message);
#endif
        return;
    }
#endif  // TRANTOR_SPDLOG_SUPPORT
    if (sourceFile_.data_)
        logStream_ << T(" - ", 3) << sourceFile_ << ':' << fileLine_ << '\n';
    else
        logStream_ << '\n';
    if (index_ < 0)
    {
        auto &oFunc = Logger::outputFunc_();
        if (!oFunc)
            return;
        oFunc(logStream_.bufferData(), logStream_.bufferLength());
        if (level_ >= kError)
            Logger::flushFunc_()();
    }
    else
    {
        auto &oFunc = Logger::outputFunc_(index_);
        if (!oFunc)
            return;
        oFunc(logStream_.bufferData(), logStream_.bufferLength());
        if (level_ >= kError)
            Logger::flushFunc_(index_)();
    }
}
LogStream &Logger::stream()
{
    return logStream_;
}
