#include <gtest/gtest.h>

#include <trantor/utils/Utilities.h>

#include <string>
#include <iostream>
using namespace trantor;
using namespace trantor::utils;

TEST(Hash, MD5)
{
    EXPECT_EQ(toHexString(md5("hello")), "5D41402ABC4B2A76B9719D911017C592");
    EXPECT_EQ(toHexString(md5("trantor")), "95FC641C9E629D2854B0B60F5A51E1FD");
}

TEST(Hash, SHA1)
{
    EXPECT_EQ(toHexString(sha1("hello")),
              "AAF4C61DDCC5E8A2DABEDE0F3B482CD9AEA9434D");
    EXPECT_EQ(toHexString(sha1("trantor")),
              "A9E084054D439FCD87D2438FB5FE4DDD7D8CC204");
}

TEST(Hash, SHA256)
{
    EXPECT_EQ(
        toHexString(sha256("hello")),
        "2CF24DBA5FB0A30E26E83B2AC5B9E29E1B161E5C1FA7425E73043362938B9824");
    EXPECT_EQ(
        toHexString(sha256("trantor")),
        "C72002E712A3BA6D60125D4B3D0B816758FBDCA98F2A892077BD4182E71CF6F5");
}

TEST(Hash, SHA3)
{
    EXPECT_EQ(
        toHexString(sha3("hello")),
        "3338BE694F50C5F338814986CDF0686453A888B84F424D792AF4B9202398F392");
    EXPECT_EQ(
        toHexString(sha3("trantor")),
        "135E1D2372F0A48525E09D47C6FFCA14077D8C5A0905410FA81C30ED9AFF696A");
}

TEST(Hash, BLAKE2b)
{
    EXPECT_EQ(
        toHexString(blake2b("hello")),
        "324DCF027DD4A30A932C441F365A25E86B173DEFA4B8E58948253471B81B72CF");
    EXPECT_EQ(
        toHexString(blake2b("trantor")),
        "2D03B3D7E76C52DD7A32689ADE4406798B50BC5B09428E3F90F56182898873C8");
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}