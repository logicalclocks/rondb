#include <botan/hash.h>
#include <trantor/utils/Utilities.h>

#include <cassert>

namespace trantor
{
namespace utils
{
Hash128 md5(const void* data, size_t len)
{
    Hash128 hash;
    auto md5 = Botan::HashFunction::create("MD5");
    md5->update((const unsigned char*)data, len);
    md5->final((unsigned char*)&hash);
    return hash;
}

Hash160 sha1(const void* data, size_t len)
{
    Hash160 hash;
    auto sha1 = Botan::HashFunction::create("SHA-1");
    sha1->update((const unsigned char*)data, len);
    sha1->final((unsigned char*)&hash);
    return hash;
}

Hash256 sha256(const void* data, size_t len)
{
    Hash256 hash;
    auto sha256 = Botan::HashFunction::create("SHA-256");
    sha256->update((const unsigned char*)data, len);
    sha256->final((unsigned char*)&hash);
    return hash;
}

Hash256 sha3(const void* data, size_t len)
{
    Hash256 hash;
    auto sha3 = Botan::HashFunction::create("SHA-3(256)");
    assert(sha3 != nullptr);
    sha3->update((const unsigned char*)data, len);
    sha3->final((unsigned char*)&hash);
    return hash;
}

Hash256 blake2b(const void* data, size_t len)
{
    Hash256 hash;
    auto blake2b = Botan::HashFunction::create("BLAKE2b(256)");
    assert(blake2b != nullptr);
    blake2b->update((const unsigned char*)data, len);
    blake2b->final((unsigned char*)&hash);
    return hash;
}

}  // namespace utils
}  // namespace trantor