#include <trantor/utils/Logger.h>
#include <trantor/utils/Utilities.h>
#include <trantor/net/TcpConnection.h>
#include <trantor/net/inner/TLSProvider.h>

#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/bio.h>
#include <openssl/x509v3.h>

#include <sys/stat.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <list>
#include <unordered_map>
#include <limits>
#include "callbacks.h"

using namespace trantor;

// Force OpenSSL to initialize before main() is called
static bool sslInitFlag = []() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    SSL_library_init();
    OpenSSL_add_all_algorithms();
    SSL_load_error_strings();
    ERR_load_BIO_strings();
    ERR_load_crypto_strings();
#elif defined(LIBRESSL_VERSION_NUMBER)
    // LibreSSL needs explicit de-init
    atexit(OPENSSL_cleanup);
#endif
    return true;
}();

namespace internal
{
static bool isIpLiteral(const std::string &hostname)
{
    struct in_addr ipv4;
    struct in6_addr ipv6;
    return inet_pton(AF_INET, hostname.c_str(), &ipv4) == 1 ||
           inet_pton(AF_INET6, hostname.c_str(), &ipv6) == 1;
}

#ifdef _WIN32
// Code yanked from stackoverflow
// https://stackoverflow.com/questions/9507184/can-openssl-on-windows-use-the-system-certificate-store
inline bool loadWindowsSystemCert(X509_STORE *store)
{
    auto hStore = CertOpenSystemStoreW((HCRYPTPROV_LEGACY)NULL, L"ROOT");

    if (!hStore)
    {
        return false;
    }

    PCCERT_CONTEXT pContext = NULL;
    while ((pContext = CertEnumCertificatesInStore(hStore, pContext)) !=
           nullptr)
    {
        auto encoded_cert =
            static_cast<const unsigned char *>(pContext->pbCertEncoded);

        auto x509 = d2i_X509(NULL, &encoded_cert, pContext->cbCertEncoded);
        if (x509)
        {
            X509_STORE_add_cert(store, x509);
            X509_free(x509);
        }
    }

    CertFreeCertificateContext(pContext);
    CertCloseStore(hStore, 0);

    return true;
}
#endif

static bool validatePeerCertificate(SSL *ssl,
                                    X509 *cert,
                                    const std::string &hostname,
                                    bool allowBrokenChain,
                                    bool isServer)
{
    assert(ssl != nullptr);
    assert(cert != nullptr);
    LOG_TRACE << "Validating peer certificate";

    if (!isServer && !hostname.empty())
    {
        // X509_check_host() only matches DNS names. Try the textual IP helper
        // first so IPv4 and IPv6 literals are checked against iPAddress SANs.
        // X509_check_ip_asc() returns -2 when hostname is not an IP literal.
        int rc = X509_check_ip_asc(cert, hostname.c_str(), 0);
        if (rc == -2)
        {
            rc = X509_check_host(
                cert, hostname.data(), hostname.size(), 0, nullptr);
        }
        if (rc != 1)
        {
            LOG_TRACE << "Peer certificate does not match hostname: "
                      << hostname;
            return false;
        }
    }

    auto result = SSL_get_verify_result(ssl);
#if OPENSSL_VERSION_NUMBER < 0x10100000L || defined(LIBRESSL_VERSION_NUMBER)
    const ASN1_TIME *notBeforeTime = X509_get_notBefore(cert);
    const ASN1_TIME *notAfterTime = X509_get_notAfter(cert);
#else
    const ASN1_TIME *notBeforeTime = X509_get0_notBefore(cert);
    const ASN1_TIME *notAfterTime = X509_get0_notAfter(cert);
#endif
    const int notBefore = X509_cmp_current_time(notBeforeTime);
    const int notAfter = X509_cmp_current_time(notAfterTime);
    // X509_cmp_current_time() returns zero for an invalid ASN.1 time, a
    // positive value for a future time, and a negative value for a past time.
    if (notBefore == 0 || notAfter == 0 || notBefore > 0 || notAfter < 0)
    {
        LOG_TRACE << "Peer certificate date validation failed";
        return false;
    }

    if (result != X509_V_OK && !allowBrokenChain)
    {
        LOG_TRACE << "cert error code: " << result;
        LOG_ERROR << "Peer certificate is not valid";
        return false;
    }

    return true;
}

static bool isCertificateHandshakeError(unsigned long error)
{
    if (error == 0 || ERR_GET_LIB(error) != ERR_LIB_SSL)
        return false;
    const auto reason = ERR_GET_REASON(error);
#ifdef SSL_R_CERTIFICATE_VERIFY_FAILED
    if (reason == SSL_R_CERTIFICATE_VERIFY_FAILED)
        return true;
#endif
#ifdef SSL_R_PEER_DID_NOT_RETURN_A_CERTIFICATE
    if (reason == SSL_R_PEER_DID_NOT_RETURN_A_CERTIFICATE)
        return true;
#endif
#ifdef SSL_R_NO_CERTIFICATE_RETURNED
    if (reason == SSL_R_NO_CERTIFICATE_RETURNED)
        return true;
#endif
#ifdef SSL_R_NO_CERTIFICATES_RETURNED
    if (reason == SSL_R_NO_CERTIFICATES_RETURNED)
        return true;
#endif
#ifdef SSL_R_TLSV13_ALERT_CERTIFICATE_REQUIRED
    if (reason == SSL_R_TLSV13_ALERT_CERTIFICATE_REQUIRED)
        return true;
#endif
#ifdef SSL_R_SSLV3_ALERT_BAD_CERTIFICATE
    if (reason == SSL_R_SSLV3_ALERT_BAD_CERTIFICATE)
        return true;
#endif
#ifdef SSL_R_SSLV3_ALERT_CERTIFICATE_EXPIRED
    if (reason == SSL_R_SSLV3_ALERT_CERTIFICATE_EXPIRED)
        return true;
#endif
#ifdef SSL_R_SSLV3_ALERT_CERTIFICATE_REVOKED
    if (reason == SSL_R_SSLV3_ALERT_CERTIFICATE_REVOKED)
        return true;
#endif
#ifdef SSL_R_SSLV3_ALERT_CERTIFICATE_UNKNOWN
    if (reason == SSL_R_SSLV3_ALERT_CERTIFICATE_UNKNOWN)
        return true;
#endif
#ifdef SSL_R_SSLV3_ALERT_UNSUPPORTED_CERTIFICATE
    if (reason == SSL_R_SSLV3_ALERT_UNSUPPORTED_CERTIFICATE)
        return true;
#endif
#ifdef SSL_R_SSLV3_ALERT_NO_CERTIFICATE
    if (reason == SSL_R_SSLV3_ALERT_NO_CERTIFICATE)
        return true;
#endif
#ifdef SSL_R_TLS_ALERT_BAD_CERTIFICATE
    if (reason == SSL_R_TLS_ALERT_BAD_CERTIFICATE)
        return true;
#endif
#ifdef SSL_R_TLS_ALERT_CERTIFICATE_EXPIRED
    if (reason == SSL_R_TLS_ALERT_CERTIFICATE_EXPIRED)
        return true;
#endif
#ifdef SSL_R_TLS_ALERT_CERTIFICATE_REVOKED
    if (reason == SSL_R_TLS_ALERT_CERTIFICATE_REVOKED)
        return true;
#endif
#ifdef SSL_R_TLS_ALERT_CERTIFICATE_UNKNOWN
    if (reason == SSL_R_TLS_ALERT_CERTIFICATE_UNKNOWN)
        return true;
#endif
#ifdef SSL_R_TLS_ALERT_UNSUPPORTED_CERTIFICATE
    if (reason == SSL_R_TLS_ALERT_UNSUPPORTED_CERTIFICATE)
        return true;
#endif
#ifdef SSL_R_TLS_ALERT_NO_CERTIFICATE
    if (reason == SSL_R_TLS_ALERT_NO_CERTIFICATE)
        return true;
#endif
#ifdef SSL_R_TLSV1_ALERT_UNKNOWN_CA
    if (reason == SSL_R_TLSV1_ALERT_UNKNOWN_CA)
        return true;
#endif
#ifdef SSL_R_TLSV1_ALERT_CERTIFICATE_REVOKED
    if (reason == SSL_R_TLSV1_ALERT_CERTIFICATE_REVOKED)
        return true;
#endif
#ifdef SSL_R_TLSV1_ALERT_CERTIFICATE_UNKNOWN
    if (reason == SSL_R_TLSV1_ALERT_CERTIFICATE_UNKNOWN)
        return true;
#endif
#ifdef SSL_R_TLSV1_ALERT_UNSUPPORTED_CERTIFICATE
    if (reason == SSL_R_TLSV1_ALERT_UNSUPPORTED_CERTIFICATE)
        return true;
#endif
#ifdef SSL_R_TLSV1_ALERT_CERTIFICATE_UNOBTAINABLE
    if (reason == SSL_R_TLSV1_ALERT_CERTIFICATE_UNOBTAINABLE)
        return true;
#endif
#ifdef SSL_R_TLSV1_CERTIFICATE_UNOBTAINABLE
    if (reason == SSL_R_TLSV1_CERTIFICATE_UNOBTAINABLE)
        return true;
#endif
#ifdef SSL_R_TLSV1_BAD_CERTIFICATE_STATUS_RESPONSE
    if (reason == SSL_R_TLSV1_BAD_CERTIFICATE_STATUS_RESPONSE)
        return true;
#endif
#ifdef SSL_R_TLSV1_BAD_CERTIFICATE_HASH_VALUE
    if (reason == SSL_R_TLSV1_BAD_CERTIFICATE_HASH_VALUE)
        return true;
#endif
    return false;
}

static bool isDirectory(const std::string &path)
{
#ifdef _WIN32
    struct _stat info;
    return _stat(path.c_str(), &info) == 0 && (info.st_mode & _S_IFDIR) != 0;
#else
    struct stat info;
    return stat(path.c_str(), &info) == 0 && S_ISDIR(info.st_mode);
#endif
}

static std::string lastOpenSSLError()
{
    const auto error = ERR_get_error();
    if (error == 0)
        return {};
    std::array<char, 256> message{};
    ERR_error_string_n(error, message.data(), message.size());
    return message.data();
}

static void freeCertificateNames(STACK_OF(X509_NAME) * names)
{
    sk_X509_NAME_pop_free(names, X509_NAME_free);
}

static STACK_OF(X509_NAME) *
    loadCertificateNames(const std::string &path, bool directory)
{
    STACK_OF(X509_NAME) *names = sk_X509_NAME_new_null();
    if (names == nullptr)
        return nullptr;
    const int result =
        directory ? SSL_add_dir_cert_subjects_to_stack(names, path.c_str())
                  : SSL_add_file_cert_subjects_to_stack(names, path.c_str());
    if (result != 1 || sk_X509_NAME_num(names) == 0)
    {
        freeCertificateNames(names);
        return nullptr;
    }
    return names;
}

static int serverSelectProtocol(SSL *ssl,
                                const unsigned char **out,
                                unsigned char *outlen,
                                const unsigned char *in,
                                unsigned int inlen,
                                void *arg)
{
    (void)ssl;
    auto protocols = static_cast<std::vector<std::string> *>(arg);
    if (protocols->empty())
        return SSL_TLSEXT_ERR_NOACK;

    for (auto &protocol : *protocols)
    {
        const unsigned char *cur = in;
        const unsigned char *end = in + inlen;
        while (cur < end)
        {
            unsigned int len = *cur++;
            if (len > static_cast<unsigned int>(end - cur))
            {
                LOG_ERROR << "Client provided invalid protocol list in APLN";
                return SSL_TLSEXT_ERR_NOACK;
            }
            if (protocol.size() == len &&
                memcmp(cur, protocol.data(), len) == 0)
            {
                *out = cur;
                *outlen = len;
                LOG_TRACE << "Selected protocol: " << protocol;
                return SSL_TLSEXT_ERR_OK;
            }
            cur += len;
        }
    }

    // TLSPolicy requires the handshake to fail when both peers advertise
    // ALPN but have no protocol in common. This also matches Botan.
    return SSL_TLSEXT_ERR_ALERT_FATAL;
}

}  // namespace internal

namespace trantor
{
struct SSLContext
{
    SSLContext(
        bool useOldTLS,
        const std::vector<std::pair<std::string, std::string>> &sslConfCmds,
        bool server)
        : isServer(server), sessionCacheId(nextSessionCacheId())
    {
#if OPENSSL_VERSION_NUMBER < 0x10100000L || defined(LIBRESSL_VERSION_NUMBER)
        ctx_.reset(SSL_CTX_new(SSLv23_method()));
#else
        ctx_.reset(SSL_CTX_new(TLS_method()));
#endif
        if (ctx_ == nullptr)
            throw std::runtime_error("Failed to create SSL context");

        // Establish Trantor's defaults first. Explicit SSL configuration
        // commands below are the final override.
        if (SSL_CTX_set_cipher_list(ctx_.get(),
                                    "MEDIUM:HIGH:!aNULL:!MD5:!RC4:!3DES") != 1)
            throw std::runtime_error("Failed to select secure ciphers");

        if (!useOldTLS)
        {
#if OPENSSL_VERSION_NUMBER >= 0x10101000L
            // Preserve stricter protocol settings from sslConfCmds while
            // enforcing TLS 1.2 as the minimum secure default.
            const auto minProtoVersion =
                SSL_CTX_get_min_proto_version(ctx_.get());
            if (minProtoVersion == 0 || minProtoVersion < TLS1_2_VERSION)
            {
                SSL_CTX_set_min_proto_version(ctx_.get(), TLS1_2_VERSION);
            }
#else
            const auto opt = SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1 |
                             SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3;
            SSL_CTX_set_options(ctx_.get(), opt);
#endif
        }
        else
        {
            LOG_WARN << "TLS 1.1 and below enabled. They are considered "
                        "obsolete, insecure standards and should only be "
                        "used for legacy purpose.";
        }

#ifdef LIBRESSL_VERSION_NUMBER
        if (!sslConfCmds.empty())
            LOG_WARN << "LibreSSL does not support SSL configuration commands";
#else
        std::unique_ptr<SSL_CONF_CTX, decltype(&SSL_CONF_CTX_free)> cctx(
            SSL_CONF_CTX_new(), SSL_CONF_CTX_free);
        if (!cctx)
            throw std::runtime_error(
                "Failed to create SSL configuration context");
        SSL_CONF_CTX_set_flags(cctx.get(),
                               server ? SSL_CONF_FLAG_SERVER
                                      : SSL_CONF_FLAG_CLIENT);
        SSL_CONF_CTX_set_flags(cctx.get(), SSL_CONF_FLAG_CERTIFICATE);
        SSL_CONF_CTX_set_flags(cctx.get(), SSL_CONF_FLAG_FILE);
        SSL_CONF_CTX_set_ssl_ctx(cctx.get(), ctx_.get());
        for (const auto &cmd : sslConfCmds)
        {
            ERR_clear_error();
            const int result =
                SSL_CONF_cmd(cctx.get(), cmd.first.c_str(), cmd.second.c_str());
            if (result <= 0)
            {
                auto message = "Invalid SSL configuration command '" +
                               cmd.first + "' with value '" + cmd.second +
                               "' (result " + std::to_string(result) + ")";
                const auto detail = internal::lastOpenSSLError();
                if (!detail.empty())
                    message += ": " + detail;
                throw std::runtime_error(message);
            }
        }
        ERR_clear_error();
        if (SSL_CONF_CTX_finish(cctx.get()) != 1)
        {
            auto message = std::string("Failed to finish SSL configuration");
            const auto detail = internal::lastOpenSSLError();
            if (!detail.empty())
                message += ": " + detail;
            throw std::runtime_error(message);
        }
#endif
        if (isServer)
        {
            // A server that requests client certificates needs a non-empty
            // context for resumable sessions. It separates sessions belonging
            // to distinct TLS contexts and avoids OpenSSL aborting a handshake
            // with SSL_R_SESSION_ID_CONTEXT_UNINITIALIZED.
            std::array<unsigned char, SSL_MAX_SID_CTX_LENGTH> sessionIdContext;
            if (!utils::secureRandomBytes(sessionIdContext.data(),
                                          sessionIdContext.size()) ||
                SSL_CTX_set_session_id_context(ctx_.get(),
                                               sessionIdContext.data(),
                                               sessionIdContext.size()) != 1)
                throw std::runtime_error(
                    "Failed to initialize SSL session-ID context");
        }
    }
    std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> ctx_{nullptr,
                                                           SSL_CTX_free};

    SSL_CTX *ctx() const
    {
        return ctx_.get();
    }

    static uint64_t nextSessionCacheId()
    {
        static std::atomic<uint64_t> nextId{1};
        return nextId.fetch_add(1, std::memory_order_relaxed);
    }

    bool isServer{false};
    const uint64_t sessionCacheId;
    std::vector<std::string> alpnProtocols;
    ServerCertificateProvider certificateProvider;
    std::string hostname;
    bool validate{true};
    bool allowBrokenChain{false};
    bool requestClientCertificate{false};
    bool requireClientCertificate{false};
};

struct OpenSSLCertificate : public Certificate
{
    OpenSSLCertificate(X509 *cert) : cert_(cert)
    {
        assert(cert_);
    }
    ~OpenSSLCertificate()
    {
        X509_free(cert_);
    }
    virtual std::string sha1Fingerprint() const override
    {
        std::string sha1;
        unsigned char md[EVP_MAX_MD_SIZE];
        unsigned int n = 0;
        if (X509_digest(cert_, EVP_sha1(), md, &n))
        {
            sha1.resize(n * 3);
            for (unsigned int i = 0; i < n; i++)
            {
                snprintf(&sha1[i * 3], 4, "%02X:", md[i]);
            }
            sha1.resize(sha1.size() - 1);
        }
        else
        {
            // handle error
            // LOG_ERROR << "X509_digest failed";
        }
        return sha1;
    }

    virtual std::string sha256Fingerprint() const override
    {
        std::string sha256;
        unsigned char md[EVP_MAX_MD_SIZE];
        unsigned int n = 0;
        if (X509_digest(cert_, EVP_sha256(), md, &n))
        {
            sha256.resize(n * 3);
            for (unsigned int i = 0; i < n; i++)
            {
                snprintf(&sha256[i * 3], 4, "%02X:", md[i]);
            }
            sha256.resize(sha256.size() - 1);
        }
        else
        {
            // handle error
            // LOG_ERROR << "X509_digest failed";
        }
        return sha256;
    }

    virtual std::string pem() const override
    {
        std::string pem;
        BIO *bio = BIO_new(BIO_s_mem());
        if (bio)
        {
            PEM_write_bio_X509(bio, cert_);
            char *data = nullptr;
            long len = BIO_get_mem_data(bio, &data);
            if (len > 0)
            {
                pem.assign(data, len);
            }
            else
            {
                // handle error
                // LOG_ERROR << "BIO_get_mem_data failed";
            }
            BIO_free(bio);
        }
        else
        {
            // handle error
            // LOG_ERROR << "BIO_new failed";
        }
        return pem;
    }
    X509 *cert_ = nullptr;
};

CertificatePtr Certificate::fromPem(const std::string &pem)
{
    BIO *bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
    if (!bio)
        return nullptr;
    X509 *cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if (!cert)
        return nullptr;
    return std::make_shared<OpenSSLCertificate>(cert);
}

namespace
{
bool loadCertificatePem(SSL_CTX *ctx,
                        const std::string &certificatePem,
                        const std::string &privateKeyPem)
{
    BIO *certBio = BIO_new_mem_buf(certificatePem.data(),
                                   static_cast<int>(certificatePem.size()));
    if (!certBio)
        return false;
    X509 *leaf = PEM_read_bio_X509_AUX(certBio, nullptr, nullptr, nullptr);
    if (!leaf || SSL_CTX_use_certificate(ctx, leaf) != 1)
    {
        X509_free(leaf);
        BIO_free(certBio);
        return false;
    }
    X509_free(leaf);
    if (SSL_CTX_clear_extra_chain_certs(ctx) != 1)
    {
        BIO_free(certBio);
        return false;
    }
    while (X509 *chain = PEM_read_bio_X509(certBio, nullptr, nullptr, nullptr))
    {
        if (SSL_CTX_add_extra_chain_cert(ctx, chain) != 1)
        {
            X509_free(chain);
            BIO_free(certBio);
            return false;
        }
    }
    ERR_clear_error();  // EOF terminates the PEM chain.
    BIO_free(certBio);

    const auto &keyPem = privateKeyPem.empty() ? certificatePem : privateKeyPem;
    BIO *keyBio =
        BIO_new_mem_buf(keyPem.data(), static_cast<int>(keyPem.size()));
    if (!keyBio)
        return false;
    EVP_PKEY *key = PEM_read_bio_PrivateKey(keyBio, nullptr, nullptr, nullptr);
    BIO_free(keyBio);
    if (!key || SSL_CTX_use_PrivateKey(ctx, key) != 1)
    {
        EVP_PKEY_free(key);
        return false;
    }
    EVP_PKEY_free(key);
    return SSL_CTX_check_private_key(ctx) == 1;
}

bool loadCertificatePem(SSL *ssl,
                        const std::string &certificatePem,
                        const std::string &privateKeyPem)
{
    BIO *certBio = BIO_new_mem_buf(certificatePem.data(),
                                   static_cast<int>(certificatePem.size()));
    if (!certBio)
        return false;
    X509 *leaf = PEM_read_bio_X509_AUX(certBio, nullptr, nullptr, nullptr);
    if (!leaf || SSL_use_certificate(ssl, leaf) != 1)
    {
        X509_free(leaf);
        BIO_free(certBio);
        return false;
    }
    X509_free(leaf);
    if (SSL_clear_chain_certs(ssl) != 1)
    {
        BIO_free(certBio);
        return false;
    }
    while (X509 *chain = PEM_read_bio_X509(certBio, nullptr, nullptr, nullptr))
    {
        if (SSL_add1_chain_cert(ssl, chain) != 1)
        {
            X509_free(chain);
            BIO_free(certBio);
            return false;
        }
        X509_free(chain);
    }
    ERR_clear_error();
    BIO_free(certBio);

    const auto &keyPem = privateKeyPem.empty() ? certificatePem : privateKeyPem;
    BIO *keyBio =
        BIO_new_mem_buf(keyPem.data(), static_cast<int>(keyPem.size()));
    if (!keyBio)
        return false;
    EVP_PKEY *key = PEM_read_bio_PrivateKey(keyBio, nullptr, nullptr, nullptr);
    BIO_free(keyBio);
    if (!key || SSL_use_PrivateKey(ssl, key) != 1)
    {
        EVP_PKEY_free(key);
        return false;
    }
    EVP_PKEY_free(key);
    return SSL_check_private_key(ssl) == 1;
}

int acceptUnverifiedPeerCertificate(int, X509_STORE_CTX *)
{
    return 1;
}
}  // namespace

class SessionManager
{
    struct SessionData
    {
        SSL_SESSION *session = nullptr;
        std::string key;
        std::chrono::steady_clock::time_point expiresAt;
    };

  public:
    ~SessionManager()
    {
        for (auto &session : sessions_)
        {
            SSL_SESSION_free(session.session);
        }
    }

    void store(uint64_t contextId,
               const std::string &hostname,
               InetAddress peerAddr,
               SSL_SESSION *session)
    {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
        {
            std::lock_guard<std::mutex> lock(mutex_);
            removeExpiredSessions();
            auto key = toKey(contextId, hostname, peerAddr);
            auto it = sessionMap_.find(key);
            if (it != sessionMap_.end())
            {
                SSL_SESSION_free(it->second->session);
                sessions_.erase(it->second);
                sessionMap_.erase(it);
            }

            SSL_SESSION_up_ref(session);
            sessions_.push_front(
                SessionData{session,
                            key,
                            std::chrono::steady_clock::now() +
                                std::chrono::seconds(sessionTimeout_)});
            sessionMap_[key] = sessions_.begin();
            removeExcessSession();
        }
#else
        (void)contextId;
        (void)hostname;
        (void)peerAddr;
        (void)session;
        assert(false && "not support under ancient openssl");
#endif
    }

    // Returns a session with an additional reference held by the caller.
    // Caller must SSL_SESSION_free() when done. Required because the entry
    // in sessionMap_ may be evicted/replaced/expired by another thread the
    // moment we release the mutex, so the SessionManager's reference is not
    // a stable ownership root for the returned pointer.
    SSL_SESSION *get(uint64_t contextId,
                     const std::string &hostname,
                     InetAddress peerAddr)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        removeExpiredSessions();
        auto it = sessionMap_.find(toKey(contextId, hostname, peerAddr));
        if (it == sessionMap_.end())
            return nullptr;
        SSL_SESSION *s = it->second->session;
        SSL_SESSION_up_ref(s);
#if OPENSSL_VERSION_NUMBER >= 0x10101000L
        // TLS 1.3 tickets should not be offered on multiple connections.
        // Transfer the cache's reference out on checkout while retaining the
        // caller reference acquired above. TLS 1.2 sessions remain reusable.
        if (SSL_SESSION_get_protocol_version(s) >= TLS1_3_VERSION)
        {
            SSL_SESSION_free(it->second->session);
            sessions_.erase(it->second);
            sessionMap_.erase(it);
        }
#endif
        return s;
    }

    void remove(uint64_t contextId,
                const std::string &hostname,
                InetAddress peerAddr,
                SSL_SESSION *expected)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = sessionMap_.find(toKey(contextId, hostname, peerAddr));
        if (it == sessionMap_.end() || it->second->session != expected)
            return;
        SSL_SESSION_free(it->second->session);
        sessions_.erase(it->second);
        sessionMap_.erase(it);
    }

    void removeExpiredSessions()
    {
        const auto now = std::chrono::steady_clock::now();
        while (!sessions_.empty())
        {
            auto it = std::prev(sessions_.end());
            if (it->expiresAt > now)
                break;
            SSL_SESSION_free(it->session);
            sessionMap_.erase(it->key);
            sessions_.erase(it);
        }
    }

    void removeExcessSession()
    {
        assert(maxSessions_ > 0);
        assert(maxExtendSize_ > 0);
        if (sessions_.size() < size_t(maxSessions_ + maxExtendSize_))
            return;
        while (sessions_.size() > size_t(maxSessions_))
        {
            auto it = sessions_.end();
            it--;
            SSL_SESSION_free(it->session);
            sessionMap_.erase(it->key);
            sessions_.erase(it);
        }
    }

    std::string toKey(uint64_t contextId,
                      const std::string &hostname,
                      InetAddress peerAddr)
    {
        // Prefixing hostname with its length keeps the hostname/address
        // boundary unambiguous even though no separator follows hostname.
        return std::to_string(contextId) + ":" +
               std::to_string(hostname.size()) + ":" + hostname +
               peerAddr.toIpPort();
    }

    std::mutex mutex_;
    int maxSessions_ = 150;
    int maxExtendSize_ = 20;
    int sessionTimeout_ = 3600;
    std::list<SessionData> sessions_;
    std::unordered_map<std::string, std::list<SessionData>::iterator>
        sessionMap_;
};

}  // namespace trantor

static SessionManager sessionManager;

#if OPENSSL_VERSION_NUMBER >= 0x10101000L
static bool canCacheSessionAfterHandshake(SSL_SESSION *session)
{
    if (session == nullptr)
        return false;
    return SSL_SESSION_is_resumable(session) == 1;
}
#endif

struct OpenSSLProvider : public TLSProvider, public NonCopyable
{
    OpenSSLProvider(TcpConnection *conn, TLSPolicyPtr policy, SSLContextPtr ctx)
        : TLSProvider(conn, std::move(policy), std::move(ctx))
    {
        rbio_ = BIO_new(BIO_s_mem());
        wbio_ = BIO_new(BIO_s_mem());
        ssl_ = SSL_new(contextPtr_->ctx());
        assert(ssl_);
        assert(rbio_);
        assert(wbio_);
        SSL_set_bio(ssl_, rbio_, wbio_);
        SSL_set_ex_data(ssl_, providerIndex(), this);
        SSL_set_mode(ssl_,
                     SSL_MODE_ENABLE_PARTIAL_WRITE |
                         SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
        if (!contextPtr_->isServer && !contextPtr_->hostname.empty() &&
            !internal::isIpLiteral(contextPtr_->hostname))
            SSL_set_tlsext_host_name(ssl_, contextPtr_->hostname.c_str());
    }

    virtual ~OpenSSLProvider()
    {
        releaseConfiguredSession(false);
        SSL_free(ssl_);
    }

    virtual void startEncryption() override
    {
        if (started_ || processedSslError_)
            return;
        started_ = true;
        if (contextPtr_->isServer)
        {
            assert(ssl_);
            SSL_set_accept_state(ssl_);
        }
        else
        {
            assert(ssl_);

            const auto &protocols = contextPtr_->alpnProtocols;
            if (!protocols.empty())
            {
                std::string alpnList;
                alpnList.reserve(24);  // some reasonable size
                for (const auto &proto : contextPtr_->alpnProtocols)
                {
                    char ch = static_cast<char>(proto.size());
                    alpnList.push_back(ch);
                    alpnList.append(proto);
                }
                if (SSL_set_alpn_protos(
                        ssl_,
                        (const unsigned char *)(alpnList.data()),
                        (unsigned int)alpnList.size()) != 0)
                {
                    LOG_TRACE << "Failed to set ALPN";
                    handleSSLError(SSLError::kSSLHandshakeError);
                    dispatchEvents();
                    return;
                }
            }

            SSL_SESSION *cachedSession =
                sessionManager.get(contextPtr_->sessionCacheId,
                                   contextPtr_->hostname,
                                   conn_->peerAddr());
            if (cachedSession)
            {
                LOG_TRACE << "Using cached TLS session";
                configuredSession_ = cachedSession;
                if (SSL_set_session(ssl_, cachedSession) != 1)
                {
                    releaseConfiguredSession(true);
                    handleSSLError(SSLError::kSSLHandshakeError);
                    dispatchEvents();
                    return;
                }
            }
            SSL_set_connect_state(ssl_);
        }

        processHandshake();
        dispatchEvents();
    }

    virtual void recvData(MsgBuffer *buffer) override
    {
        if (!started_ || processedSslError_)
        {
            buffer->retrieveAll();
            return;
        }
        LOG_TRACE << "Received " << buffer->readableBytes()
                  << " bytes from lower layer";
        if (buffer->readableBytes() == 0)
            return;
        while (buffer->readableBytes() > 0)
        {
            const auto chunkSize =
                (std::min)(buffer->readableBytes(),
                           static_cast<size_t>(
                               (std::numeric_limits<int>::max)()));
            int n =
                BIO_write(rbio_, buffer->peek(), static_cast<int>(chunkSize));
            if (n <= 0)
            {
                // TODO: make the status code more specific
                handleSSLError(SSLError::kSSLHandshakeError);
                dispatchEvents();
                return;
            }

            buffer->retrieve(n);

            if (!SSL_is_init_finished(ssl_))
            {
                bool handshakeDone = processHandshake();
                if (handshakeDone)
                    processApplicationData();
            }
            else
            {
                processApplicationData();
            }
            if (processedSslError_)
            {
                buffer->retrieveAll();
                break;
            }
        }
        dispatchEvents();
    }

    virtual void close() override
    {
        if (!SSL_is_init_finished(ssl_))
            return;
        ERR_clear_error();
        const auto result = SSL_shutdown(ssl_);
        if (result < 0)
        {
            const auto error = SSL_get_error(ssl_, result);
            if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE)
                handleSSLError(SSLError::kSSLProtocolError);
        }
        sendTLSData();
        dispatchEvents();
    }

    virtual ssize_t sendData(const char *data, size_t len) override
    {
        if (processedSslError_ || !SSL_is_init_finished(ssl_))
        {
            errno = processedSslError_ ? EPIPE : EAGAIN;
            return processedSslError_ ? -1 : 0;
        }
        if (getBufferedData().readableBytes() != 0)
        {
            errno = EAGAIN;
            return 0;
        }
        // Limit the size of the data we send in one go to avoid holding massive
        // buffers in memory.
        constexpr size_t maxSend = 64 * 1024;
        size_t hasSent = 0;
        while (hasSent < len && getBufferedData().readableBytes() == 0)
        {
            auto trunkLen = len - hasSent;
            if (trunkLen > maxSend)
                trunkLen = maxSend;
            ERR_clear_error();
            int n = SSL_write(ssl_, data + hasSent, (int)trunkLen);
            if (n <= 0 && len != 0)
            {
                const auto error = SSL_get_error(ssl_, n);
                if (error != SSL_ERROR_WANT_READ &&
                    error != SSL_ERROR_WANT_WRITE)
                {
                    handleSSLError(SSLError::kSSLProtocolError);
                    dispatchEvents();
                    return -1;
                }
                sendTLSData();
                break;
            }
            auto num = sendTLSData();
            if (num == -1)
            {
                handleSSLError(SSLError::kSSLProtocolError);
                dispatchEvents();
                return -1;
            }
            hasSent += static_cast<size_t>(n);
        }
        dispatchEvents();
        return static_cast<ssize_t>(hasSent);
    }

    bool sendBufferedData() override
    {
        auto &buffer = getBufferedData();
        if (buffer.readableBytes() == 0)
            return true;
        const auto written =
            writeCallback_(conn_, buffer.peek(), buffer.readableBytes());
        if (written < 0)
        {
            handleSSLError(SSL_is_init_finished(ssl_)
                               ? SSLError::kSSLProtocolError
                               : SSLError::kSSLHandshakeError);
            dispatchEvents();
            return false;
        }
        buffer.retrieve(static_cast<size_t>(written));
        dispatchEvents();
        return buffer.readableBytes() == 0;
    }

    bool processHandshake()
    {
        ERR_clear_error();
        int ret = SSL_do_handshake(ssl_);
        if (ret == 1)
        {
            LOG_TRACE << "SSL handshake finished";
            LOG_TRACE << "SSL session reused: "
                      << (SSL_session_reused(ssl_) == 1);
            if (contextPtr_->isServer)
            {
                const char *sniName =
                    SSL_get_servername(ssl_, TLSEXT_NAMETYPE_host_name);
                if (sniName)
                    setSniName(sniName);

                const unsigned char *alpn = nullptr;
                unsigned int alpnlen = 0;
                SSL_get0_alpn_selected(ssl_, &alpn, &alpnlen);
                if (alpn)
                    setApplicationProtocol(std::string((char *)alpn, alpnlen));
            }
            else
            {
                setSniName(contextPtr_->hostname);
                if (!contextPtr_->alpnProtocols.empty())
                {
                    const unsigned char *alpn = nullptr;
                    unsigned int alpnlen = 0;
                    SSL_get0_alpn_selected(ssl_, &alpn, &alpnlen);
                    if (alpn)
                    {
                        assert(alpnlen > 0);
                        setApplicationProtocol(
                            std::string((char *)alpn, alpnlen));
                    }
                }
            }

#if OPENSSL_VERSION_NUMBER >= 0x30000000L && !defined(LIBRESSL_VERSION_NUMBER)
            auto cert = SSL_get1_peer_certificate(ssl_);
#else
            auto cert = SSL_get_peer_certificate(ssl_);
#endif
            bool needCert = contextPtr_->validate;
            if (cert)
                setPeerCertificate(std::make_shared<OpenSSLCertificate>(cert));
            auto *local = SSL_get_certificate(ssl_);
            if (local && X509_up_ref(local) == 1)
                setLocalCertificate(
                    std::make_shared<OpenSSLCertificate>(local));

            if (needCert)
            {
                if (cert)
                {
                    bool valid = internal::validatePeerCertificate(
                        ssl_,
                        cert,
                        contextPtr_->hostname,
                        contextPtr_->allowBrokenChain,
                        contextPtr_->isServer);
                    if (!valid)
                    {
                        LOG_TRACE
                            << "SSL handshake error: invalid peer certificate";
                        SSL_shutdown(ssl_);
                        releaseConfiguredSession(true);
                        handleSSLError(SSLError::kSSLInvalidCertificate);
                        return false;
                    }
                }
                else if (!contextPtr_->isServer ||
                         contextPtr_->requireClientCertificate)
                {
                    LOG_TRACE
                        << "SSL handshake error: no peer certificate. Cannot "
                           "perform validation";
                    SSL_shutdown(ssl_);
                    releaseConfiguredSession(true);
                    handleSSLError(SSLError::kSSLInvalidCertificate);
                    return false;
                }
            }

            releaseConfiguredSession(false);
#if OPENSSL_VERSION_NUMBER >= 0x10101000L
            if (!contextPtr_->isServer)
            {
                SSL_SESSION *session = SSL_get0_session(ssl_);
                // Cache only after application-level certificate validation.
                // TLS 1.3 may not expose a resumable session until a later
                // NewSessionTicket callback.
                if (canCacheSessionAfterHandshake(session) &&
                    SSL_session_reused(ssl_) == 0)
                {
                    sessionManager.store(contextPtr_->sessionCacheId,
                                         sniName_,
                                         conn_->peerAddr(),
                                         session);
                }
            }
#endif
            sessionCacheReady_ = true;
            handshakePending_ = true;
            sendTLSData();  // Needed to send ChangeCipherSpec
            return true;
        }
        else
        {
            int err = SSL_get_error(ssl_, ret);
            if (err == SSL_ERROR_WANT_READ)
            {
                LOG_TRACE << "SSL handshake wants to read";
                sendTLSData();
            }
            else if (err == SSL_ERROR_WANT_WRITE)
            {
                LOG_TRACE << "SSL handshake wants to write";
                sendTLSData();
            }
            else if (err == SSL_ERROR_WANT_X509_LOOKUP)
            {
                // The asynchronous SNI certificate provider will resume the
                // handshake when it supplies its PEM bundle.
            }
            else
            {
                releaseConfiguredSession(true);
                if (!processedHandshakeError_)
                    processedHandshakeError_ = true;
                else
                    return false;
                const auto opensslError = ERR_get_error();
                LOG_TRACE << "SSL handshake error: "
                          << ERR_error_string(opensslError, nullptr);
                // Do not make error-queue cleanup depend on LOG_TRACE being
                // evaluated; it is compiled out in release builds.
                ERR_clear_error();
                handleSSLError(
                    SSL_get_verify_result(ssl_) != X509_V_OK ||
                            internal::isCertificateHandshakeError(opensslError)
                        ? SSLError::kSSLInvalidCertificate
                        : SSLError::kSSLHandshakeError);
            }
        }
        return false;
    }

    static int selectServerCertificate(SSL *ssl, int *alert, void *)
    {
        auto provider = static_cast<OpenSSLProvider *>(
            SSL_get_ex_data(ssl, providerIndex()));
        if (provider == nullptr)
            return SSL_TLSEXT_ERR_NOACK;
        return provider->selectServerCertificate(alert);
    }

    static int configureServerCertificate(SSL *ssl, void *)
    {
        auto *provider = static_cast<OpenSSLProvider *>(
            SSL_get_ex_data(ssl, providerIndex()));
        int alert = SSL_AD_UNRECOGNIZED_NAME;
        return provider != nullptr && provider->selectServerCertificate(
                                          &alert) == SSL_TLSEXT_ERR_OK
                   ? 1
                   : 0;
    }

#if OPENSSL_VERSION_NUMBER >= 0x10101000L
    // NOTE: LibreSSL doesn't seem to have an resumption implementation
    // and the API does nothing. Welp. Keeping it until supported I guess
    static int newSession(SSL *ssl, SSL_SESSION *session)
    {
        auto provider = static_cast<OpenSSLProvider *>(
            SSL_get_ex_data(ssl, providerIndex()));
        if (provider != nullptr && provider->sessionCacheReady_ &&
            !provider->contextPtr_->isServer &&
            SSL_SESSION_is_resumable(session))
        {
            sessionManager.store(provider->contextPtr_->sessionCacheId,
                                 provider->contextPtr_->hostname,
                                 provider->conn_->peerAddr(),
                                 session);
        }

        // SessionManager::store() takes its own reference. Tell OpenSSL that
        // the callback does not retain the reference supplied to it.
        return 0;
    }
#endif

    int selectServerCertificate(int *alert)
    {
        if (serverCertificateSelected_)
            return serverCertificateValid_ ? SSL_TLSEXT_ERR_OK
                                           : SSL_TLSEXT_ERR_ALERT_FATAL;
        serverCertificateSelected_ = true;
        const char *name = SSL_get_servername(ssl_, TLSEXT_NAMETYPE_host_name);
        try
        {
            const auto certificate =
                contextPtr_->certificateProvider(name ? name : "");
            if (!certificate.certificatePem.empty() &&
                !certificate.privateKeyPem.empty() &&
                loadCertificatePem(ssl_,
                                   certificate.certificatePem,
                                   certificate.privateKeyPem))
            {
                serverCertificateValid_ = true;
                return SSL_TLSEXT_ERR_OK;
            }
        }
        catch (const std::exception &e)
        {
            LOG_ERROR << "Server certificate provider failed: " << e.what();
        }
        catch (...)
        {
            LOG_ERROR << "Server certificate provider failed";
        }
        *alert = SSL_AD_UNRECOGNIZED_NAME;
        return SSL_TLSEXT_ERR_ALERT_FATAL;
    }

    static int providerIndex()
    {
        static const int index =
            SSL_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr);
        return index;
    }

    void processApplicationData()
    {
        constexpr size_t maxSingleRead = 128 * 1024;
        constexpr size_t maxWritibleBytes = (std::numeric_limits<int>::max)();
        while (true)
        {
            auto pending = BIO_pending(rbio_);
            // horrible syntax, because MSVC
            pending = (std::max)(1024, pending);
            recvBuffer_.ensureWritableBytes(
                (std::min)(maxSingleRead, (size_t)pending));
            // clamp to int, because that's what SSL_read accepts
            const size_t wrtibleSize =
                (std::min)(maxWritibleBytes, recvBuffer_.writableBytes());
            ERR_clear_error();
            int n = SSL_read(ssl_, recvBuffer_.beginWrite(), (int)wrtibleSize);
            const int error = n <= 0 ? SSL_get_error(ssl_, n) : SSL_ERROR_NONE;
            const int shutdownState = SSL_get_shutdown(ssl_);
            if (n == 0 && (shutdownState & SSL_RECEIVED_SHUTDOWN))
            {
                LOG_TRACE << "SSL connection closed by peer";
                closePending_ = true;
                return;
            }
            else if (n > 0)
            {
                recvBuffer_.hasWritten(n);
                LOG_TRACE << "Received " << n << " bytes from SSL";
                plaintextPending_ = true;
            }
            else if (n <= 0)
            {
                if (error == SSL_ERROR_ZERO_RETURN)
                {
                    // Clean shutdown
                    LOG_TRACE << "SSL connection closed cleanly";
                    closePending_ = true;
                    return;
                }
                if (error == SSL_ERROR_SSL || error == SSL_ERROR_SYSCALL)
                {
                    handleSSLError(SSLError::kSSLProtocolError);
                }
                else if (error == SSL_ERROR_WANT_READ ||
                         error == SSL_ERROR_WANT_WRITE)
                {
                    // SSL_read() can generate protocol output, for example a
                    // TLS 1.3 KeyUpdate response. Do not leave it stranded in
                    // the write BIO while waiting for more socket input.
                    sendTLSData();
                }
                return;
            }
        }
    }

    ssize_t sendTLSData()
    {
        void *data = nullptr;
        int len = BIO_get_mem_data(wbio_, &data);
        if (len < 0 || data == nullptr)
            return -1;
        if (len == 0)
            return 0;

        // Ciphertext already accepted by the transport must remain ahead of
        // records generated later by SSL. In particular, SSL_read() may emit
        // protocol records while an earlier socket write is still buffered.
        if (getBufferedData().readableBytes() != 0)
        {
            appendToWriteBuffer(static_cast<const char *>(data), len);
            (void)BIO_reset(wbio_);
            return len;
        }

        int n = writeCallback_(conn_, data, len);

        if (n >= 0)
        {
            appendToWriteBuffer((char *)data + n, len - n);
        }
        (void)BIO_reset(wbio_);
        if (n < 0)
        {
            if (!processedSslError_)
            {
                processedSslError_ = true;
                pendingError_ = true;
                pendingErrorValue_ = SSL_is_init_finished(ssl_)
                                         ? SSLError::kSSLProtocolError
                                         : SSLError::kSSLHandshakeError;
            }
            return -1;
        }
        return len;
    }

    void handleSSLError(SSLError error)
    {
        if (!SSL_is_init_finished(ssl_))
            releaseConfiguredSession(true);
        sendTLSData();

        if (!processedSslError_)
            processedSslError_ = true;
        else
            return;
        pendingError_ = true;
        pendingErrorValue_ = error;
    }

    void releaseConfiguredSession(bool removeFromCache)
    {
        if (configuredSession_ == nullptr)
            return;
        if (removeFromCache)
        {
            sessionManager.remove(contextPtr_->sessionCacheId,
                                  contextPtr_->hostname,
                                  conn_->peerAddr(),
                                  configuredSession_);
        }
        SSL_SESSION_free(configuredSession_);
        configuredSession_ = nullptr;
    }

    void dispatchEvents()
    {
        if (dispatchingEvents_)
            return;
        auto lifetime = shared_from_this();
        DispatchGuard guard(dispatchingEvents_);
        while (pendingError_ || handshakePending_ || plaintextPending_ ||
               closePending_)
        {
            if (pendingError_)
            {
                pendingError_ = false;
                if (errorCallback_)
                    errorCallback_(conn_, pendingErrorValue_);
                return;
            }
            if (handshakePending_)
            {
                handshakePending_ = false;
                if (handshakeCallback_)
                    handshakeCallback_(conn_);
                continue;
            }
            if (plaintextPending_)
            {
                plaintextPending_ = false;
                if (messageCallback_)
                    messageCallback_(conn_, &recvBuffer_);
                continue;
            }
            closePending_ = false;
            if (closeCallback_)
                closeCallback_(conn_);
        }
    }

    struct DispatchGuard
    {
        explicit DispatchGuard(bool &flag) : flag_(flag)
        {
            flag_ = true;
        }
        ~DispatchGuard()
        {
            flag_ = false;
        }
        bool &flag_;
    };

    SSL *ssl_;
    BIO *rbio_;
    BIO *wbio_;
    SSL_SESSION *configuredSession_{nullptr};
    bool processedHandshakeError_{false};
    bool processedSslError_{false};
    bool sessionCacheReady_{false};
    bool serverCertificateSelected_{false};
    bool serverCertificateValid_{false};
    bool started_{false};
    SSLError pendingErrorValue_{SSLError::kSSLHandshakeError};
    bool pendingError_{false};
    bool handshakePending_{false};
    bool plaintextPending_{false};
    bool closePending_{false};
    bool dispatchingEvents_{false};
};

std::shared_ptr<TLSProvider> trantor::newTLSProvider(TcpConnection *conn,
                                                     TLSPolicyPtr policy,
                                                     SSLContextPtr ctx)
{
    return std::make_shared<OpenSSLProvider>(conn,
                                             std::move(policy),
                                             std::move(ctx));
}

SSLContextPtr trantor::newSSLContext(const TLSPolicy &policy, bool isServer)
{
    auto ctx = std::make_shared<SSLContext>(policy.getUseOldTLS(),
                                            policy.getConfCmds(),
                                            isServer);
    // Everything used by a connection is captured in the shared context.
    // TcpServer snapshots this context in enableSSL(), just like Botan does.
    ctx->certificateProvider = policy.getServerCertificateProvider();
    ctx->alpnProtocols = policy.getAlpnProtocols();
    for (const auto &protocol : ctx->alpnProtocols)
    {
        if (protocol.empty() || protocol.size() > 255)
            throw std::runtime_error(
                "ALPN protocol names must contain between 1 and 255 bytes");
    }
    ctx->hostname = policy.getHostname();
    ctx->validate = policy.getValidate();
    ctx->allowBrokenChain = policy.getAllowBrokenChain();
    if (!policy.getCertificatePem().empty())
    {
        if (!loadCertificatePem(ctx->ctx(),
                                policy.getCertificatePem(),
                                policy.getPrivateKeyPem()))
            throw std::runtime_error("Failed to load certificate PEM");
    }
    else
    {
        const bool hasCertificate = !policy.getCertPath().empty();
        const bool hasPrivateKey = !policy.getKeyPath().empty();
        if (hasCertificate &&
            SSL_CTX_use_certificate_chain_file(ctx->ctx(),
                                               policy.getCertPath().c_str()) <=
                0)
        {
            throw std::runtime_error("Failed to load certificate " +
                                     policy.getCertPath());
        }
        if (hasPrivateKey &&
            SSL_CTX_use_PrivateKey_file(ctx->ctx(),
                                        policy.getKeyPath().c_str(),
                                        SSL_FILETYPE_PEM) <= 0)
        {
            throw std::runtime_error("Failed to load private key");
        }
        if (hasCertificate && hasPrivateKey &&
            SSL_CTX_check_private_key(ctx->ctx()) == 0)
        {
            throw std::runtime_error(
                "Private key does not match the "
                "certificate public key");
        }
    }
    if (!isServer && policy.getValidate())
    {
        SSL_CTX_set_verify(ctx->ctx(),
                           SSL_VERIFY_PEER,
                           policy.getAllowBrokenChain()
                               ? acceptUnverifiedPeerCertificate
                               : nullptr);
    }

    if (!policy.getCaPath().empty())
    {
        const bool caPathIsDirectory =
            internal::isDirectory(policy.getCaPath());
        const char *caFile =
            caPathIsDirectory ? nullptr : policy.getCaPath().c_str();
        const char *caDirectory =
            caPathIsDirectory ? policy.getCaPath().c_str() : nullptr;
        if (SSL_CTX_load_verify_locations(ctx->ctx(), caFile, caDirectory) != 1)
            throw std::runtime_error("Failed to load CA certificate");

        if (isServer)
        {
            if (policy.getValidate())
            {
                ctx->requestClientCertificate = true;
                ctx->requireClientCertificate = true;
            }
            STACK_OF(X509_NAME) *cert_names =
                internal::loadCertificateNames(policy.getCaPath(),
                                               caPathIsDirectory);
            if (cert_names == nullptr)
                throw std::runtime_error(
                    "No CA names found in configured path");
            SSL_CTX_set_client_CA_list(ctx->ctx(), cert_names);
            if (policy.getValidate())
                SSL_CTX_set_verify(ctx->ctx(),
                                   SSL_VERIFY_PEER |
                                       SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
                                   policy.getAllowBrokenChain()
                                       ? acceptUnverifiedPeerCertificate
                                       : nullptr);
            LOG_TRACE << "Finished loading custom CA";
        }
    }
    else if (policy.getValidate() && policy.getUseSystemCertStore())
    {
        // A configured CA path intentionally takes precedence over the system
        // store, matching Botan and the historical OpenSSL backend.
#ifdef _WIN32
        if (!internal::loadWindowsSystemCert(
                SSL_CTX_get_cert_store(ctx->ctx())))
            throw std::runtime_error(
                "Failed to load the Windows system certificate store");
#else
        if (SSL_CTX_set_default_verify_paths(ctx->ctx()) != 1)
            throw std::runtime_error(
                "Failed to load the system certificate store");
#endif
    }

    if (isServer && policy.getPeerCertificateRequest())
    {
        ctx->requestClientCertificate = true;
        ctx->requireClientCertificate = policy.getRequirePeerCertificate();
        int mode = SSL_VERIFY_PEER;
        if (policy.getRequirePeerCertificate())
            mode |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
        SSL_CTX_set_verify(ctx->ctx(),
                           mode,
                           policy.getValidate() && !policy.getAllowBrokenChain()
                               ? nullptr
                               : acceptUnverifiedPeerCertificate);
    }

    if (!policy.getAlpnProtocols().empty() && isServer)
    {
        SSL_CTX_set_alpn_select_cb(ctx->ctx(),
                                   internal::serverSelectProtocol,
                                   &ctx->alpnProtocols);
    }

    if (isServer && ctx->certificateProvider)
    {
        SSL_CTX_set_tlsext_servername_callback(
            ctx->ctx(),
            static_cast<int (*)(SSL *, int *, void *)>(
                &OpenSSLProvider::selectServerCertificate));
        SSL_CTX_set_cert_cb(ctx->ctx(),
                            &OpenSSLProvider::configureServerCertificate,
                            nullptr);
        // A resumed handshake may not select a certificate at all. Do not
        // resume one without consulting the current runtime provider.
        SSL_CTX_set_session_cache_mode(ctx->ctx(), SSL_SESS_CACHE_OFF);
        SSL_CTX_set_options(ctx->ctx(), SSL_OP_NO_TICKET);
    }

    if (!isServer)
    {
        // Keep OpenSSL's client-side session notifications enabled while
        // storing sessions only in Trantor's cache. TLS 1.3 tickets arrive
        // after the main handshake and are delivered through this callback.
#if OPENSSL_VERSION_NUMBER >= 0x10101000L
        SSL_CTX_set_session_cache_mode(ctx->ctx(),
                                       SSL_SESS_CACHE_CLIENT |
                                           SSL_SESS_CACHE_NO_INTERNAL_STORE);
        SSL_CTX_sess_set_new_cb(ctx->ctx(), &OpenSSLProvider::newSession);
#else
        SSL_CTX_set_session_cache_mode(ctx->ctx(), SSL_SESS_CACHE_OFF);
#endif
    }

    return ctx;
}
