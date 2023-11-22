/**
 *
 *  @file Logger.h
 *  @author An Tao
 *
 *  Public header file in trantor lib.
 *
 *  Copyright 2018, An Tao.  All rights reserved.
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the License file.
 *
 *
 */

#pragma once

#include <trantor/utils/NonCopyable.h>
#include <trantor/utils/Date.h>
#include <trantor/utils/LogStream.h>
#include <trantor/exports.h>
#include <cstring>
#include <functional>
#include <iostream>
#include <vector>

#define TRANTOR_IF_(cond) for (int _r = 0; _r == 0 && (cond); _r = 1)

namespace trantor
{
/**
 * @brief This class implements log functions.
 *
 */
class TRANTOR_EXPORT Logger : public NonCopyable
{
  public:
    enum LogLevel
    {
        kTrace = 0,
        kDebug,
        kInfo,
        kWarn,
        kError,
        kFatal,
        kNumberOfLogLevels
    };

    /**
     * @brief Calculate of basename of source files in compile time.
     *
     */
    class SourceFile
    {
      public:
        template <int N>
        inline SourceFile(const char (&arr)[N]) : data_(arr), size_(N - 1)
        {
            // std::cout<<data_<<std::endl;
#ifndef _MSC_VER
            const char *slash = strrchr(data_, '/');  // builtin function
#else
            const char *slash = strrchr(data_, '\\');
#endif
            if (slash)
            {
                data_ = slash + 1;
                size_ -= static_cast<int>(data_ - arr);
            }
        }

        explicit SourceFile(const char *filename = nullptr) : data_(filename)
        {
            if (!filename)
            {
                size_ = 0;
                return;
            }
#ifndef _MSC_VER
            const char *slash = strrchr(filename, '/');
#else
            const char *slash = strrchr(filename, '\\');
#endif
            if (slash)
            {
                data_ = slash + 1;
            }
            size_ = static_cast<int>(strlen(data_));
        }

        const char *data_;
        int size_;
    };
    Logger(SourceFile file, int line);
    Logger(SourceFile file, int line, LogLevel level);
    Logger(SourceFile file, int line, bool isSysErr);
    Logger(SourceFile file, int line, LogLevel level, const char *func);

    // LOG_COMPACT only <time><ThreadID><Level>
    Logger();
    Logger(LogLevel level);
    Logger(bool isSysErr);

    ~Logger();
    Logger &setIndex(int index)
    {
        index_ = index;
        return *this;
    }
    LogStream &stream();

    /**
     * @brief Set the output function.
     *
     * @param outputFunc The function to output a log message.
     * @param flushFunc The function to flush.
     * @note Logs are output to the standard output by default.
     */
    static void setOutputFunction(
        std::function<void(const char *msg, const uint64_t len)> outputFunc,
        std::function<void()> flushFunc,
        int index = -1)
    {
        if (index < 0)
        {
            outputFunc_() = outputFunc;
            flushFunc_() = flushFunc;
        }
        else
        {
            outputFunc_(index) = outputFunc;
            flushFunc_(index) = flushFunc;
        }
    }

    /**
     * @brief Set the log level. Logs below the level are not printed.
     *
     * @param level
     */
    static void setLogLevel(LogLevel level)
    {
        logLevel_() = level;
    }

    /**
     * @brief Get the current log level.
     *
     * @return LogLevel
     */
    static LogLevel logLevel()
    {
        return logLevel_();
    }

    /**
     * @brief Check whether it shows local time or UTC time.
     */
    static bool displayLocalTime()
    {
        return displayLocalTime_();
    }

    /**
     * @brief Set whether it shows local time or UTC time. the default is UTC.
     */
    static void setDisplayLocalTime(bool showLocalTime)
    {
        displayLocalTime_() = showLocalTime;
    }

  protected:
    static void defaultOutputFunction(const char *msg, const uint64_t len)
    {
        fwrite(msg, 1, static_cast<size_t>(len), stdout);
    }
    static void defaultFlushFunction()
    {
        fflush(stdout);
    }
    void formatTime();
    static bool &displayLocalTime_()
    {
        static bool showLocalTime = false;
        return showLocalTime;
    }

    static LogLevel &logLevel_()
    {
#ifdef RELEASE
        static LogLevel logLevel = LogLevel::kInfo;
#else
        static LogLevel logLevel = LogLevel::kDebug;
#endif
        return logLevel;
    }
    static std::function<void(const char *msg, const uint64_t len)>
        &outputFunc_()
    {
        static std::function<void(const char *msg, const uint64_t len)>
            outputFunc = Logger::defaultOutputFunction;
        return outputFunc;
    }
    static std::function<void()> &flushFunc_()
    {
        static std::function<void()> flushFunc = Logger::defaultFlushFunction;
        return flushFunc;
    }
    static std::function<void(const char *msg, const uint64_t len)>
        &outputFunc_(size_t index)
    {
        static std::vector<
            std::function<void(const char *msg, const uint64_t len)>>
            outputFuncs;
        if (index < outputFuncs.size())
        {
            return outputFuncs[index];
        }
        while (index >= outputFuncs.size())
        {
            outputFuncs.emplace_back(outputFunc_());
        }
        return outputFuncs[index];
    }
    static std::function<void()> &flushFunc_(size_t index)
    {
        static std::vector<std::function<void()>> flushFuncs;
        if (index < flushFuncs.size())
        {
            return flushFuncs[index];
        }
        while (index >= flushFuncs.size())
        {
            flushFuncs.emplace_back(flushFunc_());
        }
        return flushFuncs[index];
    }
    friend class RawLogger;
    LogStream logStream_;
    Date date_{Date::now()};
    SourceFile sourceFile_;
    int fileLine_;
    LogLevel level_;
    int index_{-1};
};
class TRANTOR_EXPORT RawLogger : public NonCopyable
{
  public:
    ~RawLogger();
    RawLogger &setIndex(int index)
    {
        index_ = index;
        return *this;
    }
    LogStream &stream()
    {
        return logStream_;
    }

  private:
    LogStream logStream_;
    int index_{-1};
};
#ifdef NDEBUG
#define LOG_TRACE                                                          \
    TRANTOR_IF_(0)                                                         \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kTrace, __func__) \
        .stream()
#else
#define LOG_TRACE                                                          \
    TRANTOR_IF_(trantor::Logger::logLevel() <= trantor::Logger::kTrace)    \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kTrace, __func__) \
        .stream()
#define LOG_TRACE_TO(index)                                                \
    TRANTOR_IF_(trantor::Logger::logLevel() <= trantor::Logger::kTrace)    \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kTrace, __func__) \
        .setIndex(index)                                                   \
        .stream()

#endif

#define LOG_DEBUG                                                          \
    TRANTOR_IF_(trantor::Logger::logLevel() <= trantor::Logger::kDebug)    \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kDebug, __func__) \
        .stream()
#define LOG_DEBUG_TO(index)                                                \
    TRANTOR_IF_(trantor::Logger::logLevel() <= trantor::Logger::kDebug)    \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kDebug, __func__) \
        .setIndex(index)                                                   \
        .stream()
#define LOG_INFO                                                       \
    TRANTOR_IF_(trantor::Logger::logLevel() <= trantor::Logger::kInfo) \
    trantor::Logger(__FILE__, __LINE__).stream()
#define LOG_INFO_TO(index)                                             \
    TRANTOR_IF_(trantor::Logger::logLevel() <= trantor::Logger::kInfo) \
    trantor::Logger(__FILE__, __LINE__).setIndex(index).stream()
#define LOG_WARN \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kWarn).stream()
#define LOG_WARN_TO(index)                                      \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kWarn) \
        .setIndex(index)                                        \
        .stream()
#define LOG_ERROR \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kError).stream()
#define LOG_ERROR_TO(index)                                      \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kError) \
        .setIndex(index)                                         \
        .stream()
#define LOG_FATAL \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kFatal).stream()
#define LOG_FATAL_TO(index)                                      \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kFatal) \
        .setIndex(index)                                         \
        .stream()
#define LOG_SYSERR trantor::Logger(__FILE__, __LINE__, true).stream()
#define LOG_SYSERR_TO(index) \
    trantor::Logger(__FILE__, __LINE__, true).setIndex(index).stream()

// LOG_COMPACT_... begin block
#define LOG_COMPACT_DEBUG                                               \
    TRANTOR_IF_(trantor::Logger::logLevel() <= trantor::Logger::kDebug) \
    trantor::Logger(trantor::Logger::kDebug).stream()
#define LOG_COMPACT_DEBUG_TO(index)                                     \
    TRANTOR_IF_(trantor::Logger::logLevel() <= trantor::Logger::kDebug) \
    trantor::Logger(trantor::Logger::kDebug).setIndex(index).stream()
#define LOG_COMPACT_INFO                                               \
    TRANTOR_IF_(trantor::Logger::logLevel() <= trantor::Logger::kInfo) \
    trantor::Logger().stream()
#define LOG_COMPACT_INFO_TO(index)                                     \
    TRANTOR_IF_(trantor::Logger::logLevel() <= trantor::Logger::kInfo) \
    trantor::Logger().setIndex(index).stream()
#define LOG_COMPACT_WARN trantor::Logger(trantor::Logger::kWarn).stream()
#define LOG_COMPACT_WARN_TO(index) \
    trantor::Logger(trantor::Logger::kWarn).setIndex(index).stream()
#define LOG_COMPACT_ERROR trantor::Logger(trantor::Logger::kError).stream()
#define LOG_COMPACT_ERROR_TO(index) \
    trantor::Logger(trantor::Logger::kError).setIndex(index).stream()
#define LOG_COMPACT_FATAL trantor::Logger(trantor::Logger::kFatal).stream()
#define LOG_COMPACT_FATAL_TO(index) \
    trantor::Logger(trantor::Logger::kFatal).setIndex(index).stream()
#define LOG_COMPACT_SYSERR trantor::Logger(true).stream()
#define LOG_COMPACT_SYSERR_TO(index) \
    trantor::Logger(true).setIndex(index).stream()
// LOG_COMPACT_... end block

#define LOG_RAW trantor::RawLogger().stream()
#define LOG_RAW_TO(index) trantor::RawLogger().setIndex(index).stream()

#define LOG_TRACE_IF(cond)                                                  \
    TRANTOR_IF_((trantor::Logger::logLevel() <= trantor::Logger::kTrace) && \
                (cond))                                                     \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kTrace, __func__)  \
        .stream()
#define LOG_DEBUG_IF(cond)                                                  \
    TRANTOR_IF_((trantor::Logger::logLevel() <= trantor::Logger::kDebug) && \
                (cond))                                                     \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kDebug, __func__)  \
        .stream()
#define LOG_INFO_IF(cond)                                                  \
    TRANTOR_IF_((trantor::Logger::logLevel() <= trantor::Logger::kInfo) && \
                (cond))                                                    \
    trantor::Logger(__FILE__, __LINE__).stream()
#define LOG_WARN_IF(cond) \
    TRANTOR_IF_(cond)     \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kWarn).stream()
#define LOG_ERROR_IF(cond) \
    TRANTOR_IF_(cond)      \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kError).stream()
#define LOG_FATAL_IF(cond) \
    TRANTOR_IF_(cond)      \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kFatal).stream()

#ifdef NDEBUG
#define DLOG_TRACE                                                         \
    TRANTOR_IF_(0)                                                         \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kTrace, __func__) \
        .stream()
#define DLOG_DEBUG                                                         \
    TRANTOR_IF_(0)                                                         \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kDebug, __func__) \
        .stream()
#define DLOG_INFO  \
    TRANTOR_IF_(0) \
    trantor::Logger(__FILE__, __LINE__).stream()
#define DLOG_WARN  \
    TRANTOR_IF_(0) \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kWarn).stream()
#define DLOG_ERROR \
    TRANTOR_IF_(0) \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kError).stream()
#define DLOG_FATAL \
    TRANTOR_IF_(0) \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kFatal).stream()

#define DLOG_TRACE_IF(cond)                                                \
    TRANTOR_IF_(0)                                                         \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kTrace, __func__) \
        .stream()
#define DLOG_DEBUG_IF(cond)                                                \
    TRANTOR_IF_(0)                                                         \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kDebug, __func__) \
        .stream()
#define DLOG_INFO_IF(cond) \
    TRANTOR_IF_(0)         \
    trantor::Logger(__FILE__, __LINE__).stream()
#define DLOG_WARN_IF(cond) \
    TRANTOR_IF_(0)         \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kWarn).stream()
#define DLOG_ERROR_IF(cond) \
    TRANTOR_IF_(0)          \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kError).stream()
#define DLOG_FATAL_IF(cond) \
    TRANTOR_IF_(0)          \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kFatal).stream()
#else
#define DLOG_TRACE                                                         \
    TRANTOR_IF_(trantor::Logger::logLevel() <= trantor::Logger::kTrace)    \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kTrace, __func__) \
        .stream()
#define DLOG_DEBUG                                                         \
    TRANTOR_IF_(trantor::Logger::logLevel() <= trantor::Logger::kDebug)    \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kDebug, __func__) \
        .stream()
#define DLOG_INFO                                                      \
    TRANTOR_IF_(trantor::Logger::logLevel() <= trantor::Logger::kInfo) \
    trantor::Logger(__FILE__, __LINE__).stream()
#define DLOG_WARN \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kWarn).stream()
#define DLOG_ERROR \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kError).stream()
#define DLOG_FATAL \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kFatal).stream()

#define DLOG_TRACE_IF(cond)                                                 \
    TRANTOR_IF_((trantor::Logger::logLevel() <= trantor::Logger::kTrace) && \
                (cond))                                                     \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kTrace, __func__)  \
        .stream()
#define DLOG_DEBUG_IF(cond)                                                 \
    TRANTOR_IF_((trantor::Logger::logLevel() <= trantor::Logger::kDebug) && \
                (cond))                                                     \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kDebug, __func__)  \
        .stream()
#define DLOG_INFO_IF(cond)                                                 \
    TRANTOR_IF_((trantor::Logger::logLevel() <= trantor::Logger::kInfo) && \
                (cond))                                                    \
    trantor::Logger(__FILE__, __LINE__).stream()
#define DLOG_WARN_IF(cond) \
    TRANTOR_IF_(cond)      \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kWarn).stream()
#define DLOG_ERROR_IF(cond) \
    TRANTOR_IF_(cond)       \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kError).stream()
#define DLOG_FATAL_IF(cond) \
    TRANTOR_IF_(cond)       \
    trantor::Logger(__FILE__, __LINE__, trantor::Logger::kFatal).stream()
#endif

const char *strerror_tl(int savedErrno);
}  // namespace trantor
