#pragma once
#include <trantor/exports.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace trantor
{
struct TRANTOR_EXPORT TLSPolicy final
{
    /**
     * @brief set the ssl configuration commands. The commands will be passed
     * to the ssl library. The commands are in the form of {{key, value}}.
     * for example, {"SSL_OP_NO_SSLv2", "1"}. Not all TLS providers support
     * this feature AND the meaning of the commands may vary between TLS
     * providers.
     *
     * As of 2023-03 Only OpenSSL supports this feature. LibreSSL does not
     * nor Botan.
     */
    TLSPolicy &setConfCmds(
        const std::vector<std::pair<std::string, std::string>> &sslConfCmds)
    {
        sslConfCmds_ = sslConfCmds;
        return *this;
    }
    /**
     * @brief set the hostname to be used for SNI and certificate validation.
     */
    TLSPolicy &setHostname(const std::string &hostname)
    {
        hostname_ = hostname;
        return *this;
    }

    /**
     * @brief set the path to the certificate file. The file must be in PEM
     * format.
     */
    TLSPolicy &setCertPath(const std::string &certPath)
    {
        certPath_ = certPath;
        return *this;
    }

    /**
     * @brief set the path to the private key file. The file must be in PEM
     * format.
     */
    TLSPolicy &setKeyPath(const std::string &keyPath)
    {
        keyPath_ = keyPath;
        return *this;
    }

    /**
     * @brief set the path to the CA file or directory. The file must be in
     * PEM format.
     */
    TLSPolicy &setCaPath(const std::string &caPath)
    {
        caPath_ = caPath;
        return *this;
    }

    /**
     * @brief enables the use of the old TLS protocol (old meaning < TLS 1.2).
     * TLS providres may not support old protocols even if this option is set
     */
    TLSPolicy &setUseOldTLS(bool useOldTLS)
    {
        useOldTLS_ = useOldTLS;
        return *this;
    }

    /**
     * @brief set the list of protocols to be used for ALPN.
     *
     * @note for servers, it selects matching protocol against the client's
     * list. And the first matching protocol supplide in the parameter will be
     * selected. If no matching protocol is found, the connection will be
     * closed.
     *
     * @note for clients, it sends the list of protocols to the server.
     */
    TLSPolicy &setAlpnProtocols(const std::vector<std::string> &alpnProtocols)
    {
        alpnProtocols_ = alpnProtocols;
        return *this;
    }
    TLSPolicy &setAlpnProtocols(std::vector<std::string> &&alpnProtocols)
    {
        alpnProtocols_ = std::move(alpnProtocols);
        return *this;
    }

    /**
     * @brief Weather to use the system's certificate store.
     *
     * @note setting both not to use the system's certificate store and to
     * supply a CA path WILL LEAD TO NO CERTIFICATE VALIDATION AT ALL.
     */
    TLSPolicy &setUseSystemCertStore(bool useSystemCertStore)
    {
        useSystemCertStore_ = useSystemCertStore;
        return *this;
    }

    /**
     * @brief Enable certificate validation.
     */
    TLSPolicy &setValidate(bool enable)
    {
        validate_ = enable;
        return *this;
    }

    /**
     * @brief Allow broken chain (self-signed certificate, root CA not in
     * allowed list, etc..) but still validate the domain name and date. This
     * option has no effect if validate is false.
     *
     * @note IMPORTANT: This option makes more then self signed certificates
     * valid. It also allows certificates that are not signed by a trusted CA,
     * the CA gets revoked. But the underlying implementation may still check
     * for the type of certificate, date and hostname, etc.. To disable all
     * certificate validation, use setValidate(false).
     */
    TLSPolicy &setAllowBrokenChain(bool allow)
    {
        allowBrokenChain_ = allow;
        return *this;
    }

    // The getters
    const std::vector<std::pair<std::string, std::string>> &getConfCmds() const
    {
        return sslConfCmds_;
    }
    const std::string &getHostname() const
    {
        return hostname_;
    }
    const std::string &getCertPath() const
    {
        return certPath_;
    }
    const std::string &getKeyPath() const
    {
        return keyPath_;
    }
    const std::string &getCaPath() const
    {
        return caPath_;
    }
    bool getUseOldTLS() const
    {
        return useOldTLS_;
    }
    bool getValidate() const
    {
        return validate_;
    }
    bool getAllowBrokenChain() const
    {
        return allowBrokenChain_;
    }
    const std::vector<std::string> &getAlpnProtocols() const
    {
        return alpnProtocols_;
    }
    const std::vector<std::string> &getAlpnProtocols()
    {
        return alpnProtocols_;
    }

    bool getUseSystemCertStore() const
    {
        return useSystemCertStore_;
    }

    static std::shared_ptr<TLSPolicy> defaultServerPolicy(
        const std::string &certPath,
        const std::string &keyPath)
    {
        auto policy = std::make_shared<TLSPolicy>();
        policy->setValidate(false)
            .setUseOldTLS(false)
            .setUseSystemCertStore(false)
            .setCertPath(certPath)
            .setKeyPath(keyPath);
        return policy;
    }

    static std::shared_ptr<TLSPolicy> defaultClientPolicy(
        const std::string &hostname = "")
    {
        auto policy = std::make_shared<TLSPolicy>();
        policy->setValidate(true)
            .setUseOldTLS(false)
            .setUseSystemCertStore(true)
            .setHostname(hostname);
        return policy;
    }

  protected:
    std::vector<std::pair<std::string, std::string>> sslConfCmds_ = {};
    std::string hostname_ = "";
    std::string certPath_ = "";
    std::string keyPath_ = "";
    std::string caPath_ = "";
    std::vector<std::string> alpnProtocols_ = {};
    bool useOldTLS_ = false;  // turn into specific version
    bool validate_ = true;
    bool allowBrokenChain_ = false;
    bool useSystemCertStore_ = true;
};
using TLSPolicyPtr = std::shared_ptr<TLSPolicy>;
}  // namespace trantor