#pragma once
#include <string>
#include <memory>

namespace trantor
{
struct Certificate
{
    virtual ~Certificate() = default;
    static std::shared_ptr<Certificate> fromPem(const std::string &pem);
    virtual std::string sha1Fingerprint() const = 0;
    virtual std::string sha256Fingerprint() const = 0;
    virtual std::string pem() const = 0;
};
using CertificatePtr = std::shared_ptr<Certificate>;

}  // namespace trantor
