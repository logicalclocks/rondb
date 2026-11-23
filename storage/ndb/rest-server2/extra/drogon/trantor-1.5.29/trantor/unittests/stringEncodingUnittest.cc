#include <trantor/utils/Utilities.h>
#include <gtest/gtest.h>
#include <iostream>
using namespace trantor;

const std::string utf8Path("C:/Temp/\xE4\xB8\xAD\xE6\x96\x87");
const std::string utf8AltPath("C:\\Temp\\\xE4\xB8\xAD\xE6\x96\x87");
const std::wstring widePath(L"C:/Temp/\u4E2D\u6587");
const std::wstring windowsPath(L"C:\\Temp\\\u4E2D\u6587");
#ifdef _WIN32
const std::wstring nativePath(windowsPath);
#else   // _WIN32
const std::string nativePath(utf8Path);
#endif  // _WIN32

TEST(pathConversion, fromUtf8a)
{
    auto out = utils::fromUtf8(utf8Path);
    EXPECT_EQ(out, widePath)
        << "Error converting " << utf8Path << " to wide string";
}
TEST(pathConversion, fromUtf8b)
{
    auto out = utils::fromUtf8(utf8AltPath);
    EXPECT_EQ(out, windowsPath)
        << "Error converting " << utf8AltPath << " to wide string";
}
TEST(pathConversion, toUtf8a)
{
    auto out = utils::toUtf8(widePath);
    EXPECT_EQ(out, utf8Path)
        << "Error converting " << widePath << " from wide string to utf-8";
}
TEST(pathConversion, toUtf8b)
{
    auto out = utils::toUtf8(windowsPath);
    EXPECT_EQ(out, utf8AltPath)
        << "Error converting " << windowsPath << " from wide string to utf-8";
}
TEST(pathConversion, fromWidePath1)
{
    auto out = utils::fromWidePath(widePath);
    EXPECT_EQ(out, utf8Path)
        << "Error converting " << widePath << " from wide path to utf-8";
}
TEST(pathConversion, fromWidePath2)
{
    auto out = utils::fromWidePath(windowsPath);
#ifdef _WIN32
    EXPECT_EQ(out, utf8Path)
#else   // _WIN32
    EXPECT_EQ(out, utf8AltPath)
#endif  // _WIN32
        << "Error converting " << windowsPath << " from wide path to utf-8";
}
TEST(pathConversion, toWidePath1)
{
    auto out = utils::toWidePath(utf8Path);
#ifdef _WIN32
    EXPECT_EQ(out, windowsPath)
#else   // _WIN32
    EXPECT_EQ(out, widePath)
#endif  // _WIN32
        << "Error converting " << utf8Path << " from utf-8 path to wide string";
}
TEST(pathConversion, toWidePath2)
{
    auto out = utils::toWidePath(utf8AltPath);
    EXPECT_EQ(out, windowsPath) << "Error converting " << utf8AltPath
                                << " from utf-8 path to wide string";
}
TEST(pathConversion, toNativePath1)
{
    auto out = utils::toNativePath(utf8Path);
    EXPECT_EQ(out, nativePath)
        << "Error converting " << utf8Path << " from utf-8 path to native path";
}
TEST(pathConversion, toNativePath2)
{
    auto out = utils::toNativePath(utf8AltPath);
#ifdef _WIN32
    EXPECT_EQ(out, windowsPath) << "Error converting " << utf8AltPath
                                << " from utf-8 path to wide string";
#else   // _WIN32
    EXPECT_EQ(out, utf8AltPath) << "Error converting " << utf8AltPath
                                << " from utf-8 path (should be a noop)";
#endif  // _WIN32
}
TEST(pathConversion, toNativePath3)
{
    auto out = utils::toNativePath(windowsPath);
#ifdef _WIN32
    EXPECT_EQ(out, windowsPath) << "Error converting " << windowsPath
                                << " from wide path (should be a noop)";
#else   // _WIN32
    EXPECT_EQ(out, utf8AltPath)
        << "Error converting " << windowsPath << " from wide path to utf-8";
#endif  // _WIN32
}
TEST(pathConversion, toNativePath4)
{
    auto out = utils::toNativePath(widePath);
#ifdef _WIN32
    EXPECT_EQ(out, widePath) << "Error converting " << widePath
                             << " from wide path (should be a noop)";
#else   // _WIN32
    EXPECT_EQ(out, utf8Path)
        << "Error converting " << widePath << " from wide path to utf-8";
#endif  // _WIN32
}
TEST(pathConversion, fromNativePath1)
{
    auto out = utils::fromNativePath(nativePath);
    EXPECT_EQ(out, utf8Path)
        << "Error converting " << nativePath << " from native path to utf-8";
}
TEST(pathConversion, fromNativePath2)
{
    auto out = utils::fromNativePath(utf8Path);
    EXPECT_EQ(out, utf8Path) << "Error converting " << utf8Path
                             << " from utf-8 path (should be a noop)";
}
TEST(pathConversion, fromNativePath3)
{
    auto out = utils::fromNativePath(utf8AltPath);
    EXPECT_EQ(out, utf8AltPath) << "Error converting " << utf8AltPath
                                << " from utf-8 path (should be a noop)";
}
TEST(pathConversion, fromNativePath4)
{
    auto out = utils::fromNativePath(windowsPath);
#ifdef _WIN32
    EXPECT_EQ(out, utf8Path)
#else   // _WIN32
    EXPECT_EQ(out, utf8AltPath)
#endif  // _WIN32
        << "Error converting " << windowsPath << " from wide path to utf-8";
}
TEST(pathConversion, fromNativePath5)
{
    auto out = utils::fromNativePath(widePath);
    EXPECT_EQ(out, utf8Path)
        << "Error converting " << widePath << " from wide path to utf-8";
}
int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
