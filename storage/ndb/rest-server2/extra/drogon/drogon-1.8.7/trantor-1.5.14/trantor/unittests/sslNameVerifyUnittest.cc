#include <trantor/utils/Utilities.h>
#include <gtest/gtest.h>
#include <iostream>
using namespace trantor;
using namespace trantor::utils;

TEST(sslNameCheck, baseCases)
{
    EXPECT_EQ(verifySslName("example.com", "example.com"), true);
    EXPECT_EQ(verifySslName("example.com", "example.org"), false);
    EXPECT_EQ(verifySslName("example.com", "www.example.com"), false);
}

TEST(sslNameCheck, rfc6125Examples)
{
    EXPECT_EQ(verifySslName("*.example.com", "foo.example.com"), true);
    EXPECT_EQ(verifySslName("*.example.com", "foo.bar.example.com"), false);
    EXPECT_EQ(verifySslName("*.example.com", "example.com"), false);
    EXPECT_EQ(verifySslName("*bar.example.com", "foobar.example.com"), true);
    EXPECT_EQ(verifySslName("baz*.example.com", "baz1.example.com"), true);
    EXPECT_EQ(verifySslName("b*z.example.com", "buzz.example.com"), true);
}

TEST(sslNameCheck, rfcCounterExamples)
{
    EXPECT_EQ(verifySslName("buz*.example.com", "buaz.example.com"), false);
    EXPECT_EQ(verifySslName("*bar.example.com", "aaasdasbaz.example.com"),
              false);
    EXPECT_EQ(verifySslName("b*z.example.com", "baaaaaa.example.com"), false);
}

TEST(sslNameCheck, wildExamples)
{
    EXPECT_EQ(verifySslName("datatracker.ietf.org", "datatracker.ietf.org"),
              true);
    EXPECT_EQ(verifySslName("*.nsysu.edu.tw", "nsysu.edu.tw"), false);
    EXPECT_EQ(verifySslName("nsysu.edu.tw", "nsysu.edu.tw"), true);
}

TEST(sslNameCheck, edgeCase)
{
    EXPECT_EQ(verifySslName(".example.com", "example.com"), false);
    EXPECT_EQ(verifySslName("example.com.", "example.com."), true);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
