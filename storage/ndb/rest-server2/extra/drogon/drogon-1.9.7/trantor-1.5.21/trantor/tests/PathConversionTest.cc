#include <trantor/utils/Utilities.h>
#include <trantor/utils/Logger.h>
#include <stdlib.h>

int main()
{
    trantor::Logger::setLogLevel(trantor::Logger::kTrace);
    LOG_DEBUG << "PathConversion utils test!";

#ifdef _WIN32
    std::string utf8PathStandard("C:/Temp/\xE4\xB8\xAD\xE6\x96\x87");
    std::string utf8PathAlt("C:\\Temp\\\xE4\xB8\xAD\xE6\x96\x87");
    std::wstring widePathStandard(L"C:\\Temp\\\u4E2D\u6587");
    std::wstring widePathAlt(L"C:/Temp/\u4E2D\u6587");
    std::string utf8WidePathStandard{utf8PathAlt};
    std::string utf8WidePathAlt{utf8PathStandard};
#else   // _WIN32
    std::string utf8PathStandard("/tmp/\xE4\xB8\xAD\xE6\x96\x87");
    std::string utf8PathAlt(
        "\\tmp\\\xE4\xB8\xAD\xE6\x96\x87");  // Invalid, won't be changed
    std::wstring widePathStandard(L"/tmp/\u4E2D\u6587");
    std::wstring widePathAlt(L"\\tmp\\\u4E2D\u6587");
    std::string utf8WidePathStandard{utf8PathStandard};
    std::string utf8WidePathAlt{utf8PathAlt};
#endif  // _WIN32

    // 1. Check from/to UTF-8
#ifdef _WIN32
    if (utf8PathAlt != trantor::utils::toUtf8(widePathStandard))
#else   // _WIN32
    if (utf8PathStandard != trantor::utils::toUtf8(widePathStandard))
#endif  // _WIN32
        LOG_ERROR << "Error converting " << utf8WidePathStandard
                  << " from wide string to utf-8";
#ifdef _WIN32
    if (utf8PathStandard != trantor::utils::toUtf8(widePathAlt))
#else   // _WIN32
    if (utf8PathAlt != trantor::utils::toUtf8(widePathAlt))
#endif  // _WIN32
        LOG_ERROR << "Error converting " << utf8WidePathAlt
                  << " from wide string to utf-8";
#ifdef _WIN32
    if (widePathAlt != trantor::utils::fromUtf8(utf8PathStandard))
#else   // _WIN32
    if (widePathStandard != trantor::utils::fromUtf8(utf8PathStandard))
#endif  // _WIN32
        LOG_ERROR << "Error converting " << utf8PathStandard
                  << " from utf-8 to wide string";
#ifdef _WIN32
    if (widePathStandard != trantor::utils::fromUtf8(utf8PathAlt))
#else   // _WIN32
    if (widePathAlt != trantor::utils::fromUtf8(utf8PathAlt))
#endif  // _WIN32
        LOG_ERROR << "Error converting " << utf8PathAlt
                  << " from utf-8 to wide string";

    // 2. Check path conversion. Note: The directory separator should be changed
    // on Windows only
    if (utf8PathStandard != trantor::utils::fromWidePath(widePathStandard))
        LOG_ERROR << "Error converting " << utf8WidePathStandard
                  << " from wide path to utf-8";
#ifdef _WIN32
    if (utf8PathStandard != trantor::utils::fromWidePath(widePathAlt))
#else   // _WIN32
    if (utf8PathAlt != trantor::utils::fromWidePath(widePathAlt))
#endif  // _WIN32
        LOG_ERROR << "Error converting " << utf8WidePathAlt
                  << " from wide path to utf-8";
    if (widePathStandard != trantor::utils::toWidePath(utf8PathStandard))
        LOG_ERROR << "Error converting " << utf8WidePathStandard
                  << " from utf-8 to wide path";
#ifdef _WIN32
    if (widePathStandard != trantor::utils::toWidePath(utf8PathAlt))
#else   // _WIN32
    if (widePathAlt != trantor::utils::toWidePath(utf8PathAlt))
#endif  // _WIN32
        LOG_ERROR << "Error converting " << utf8PathAlt
                  << " from utf-8 to wide path";

    // 3. From/to native path
    auto nativePath1 = trantor::utils::toNativePath(widePathStandard);
    auto nativePath2 = trantor::utils::toNativePath(utf8PathStandard);
    if (nativePath1 != nativePath2)
        LOG_ERROR << "Error converting " << utf8PathStandard
                  << " to native path";
    if (utf8PathStandard != trantor::utils::fromNativePath(nativePath1))
        LOG_ERROR << "Error converting " << utf8PathStandard
                  << " from native to utf-8 path";
}
