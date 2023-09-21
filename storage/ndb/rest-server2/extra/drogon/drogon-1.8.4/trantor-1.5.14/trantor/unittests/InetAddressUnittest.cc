#include <trantor/net/InetAddress.h>
#include <gtest/gtest.h>
#include <string>
#include <iostream>
using namespace trantor;
TEST(InetAddress, innerIpTest)
{
    EXPECT_EQ(true, InetAddress("192.168.0.1", 0).isIntranetIp());
    EXPECT_EQ(true, InetAddress("192.168.12.1", 0).isIntranetIp());
    EXPECT_EQ(true, InetAddress("10.168.0.1", 0).isIntranetIp());
    EXPECT_EQ(true, InetAddress("10.0.0.1", 0).isIntranetIp());
    EXPECT_EQ(true, InetAddress("172.31.10.1", 0).isIntranetIp());
    EXPECT_EQ(true, InetAddress("127.0.0.1", 0).isIntranetIp());
    EXPECT_EQ(true, InetAddress("example.com", 0).isUnspecified());
    EXPECT_EQ(false, InetAddress("127.0.0.2", 0).isUnspecified());
    EXPECT_EQ(false, InetAddress("0.0.0.0", 0).isUnspecified());
}
TEST(InetAddress, toIpPortNetEndianTest)
{
    EXPECT_EQ(std::string({char(192), char(168), 0, 1, 0, 80}),
              InetAddress("192.168.0.1", 80).toIpPortNetEndian());
    EXPECT_EQ(std::string({0x20,
                           0x01,
                           0x0d,
                           char(0xb8),
                           0x33,
                           0x33,
                           0x44,
                           0x44,
                           0x55,
                           0x55,
                           0x66,
                           0x66,
                           0x77,
                           0x77,
                           char(0x88),
                           char(0x88),
                           1,
                           char(187)}),
              InetAddress("2001:0db8:3333:4444:5555:6666:7777:8888", 443, true)
                  .toIpPortNetEndian());
}
int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
