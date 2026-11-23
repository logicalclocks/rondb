#include <trantor/net/inner/TLSProvider.h>
#include <trantor/net/Certificate.h>
#include <trantor/net/TcpConnection.h>
#include <trantor/net/callbacks.h>
#include <trantor/utils/Logger.h>

#include <botan/tls_server.h>
#include <botan/tls_client.h>
#include <botan/tls_callbacks.h>
#include <botan/tls_policy.h>
#include <botan/auto_rng.h>
#include <botan/certstor.h>
#include <botan/certstor_system.h>
#include <botan/data_src.h>
#include <botan/pkcs8.h>
#include <botan/tls_exceptn.h>
#include <botan/tls_session.h>
#include <botan/pkix_types.h>
#include <botan/x509path.h>
#include <botan/tls_session_manager_memory.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace trantor;

static thread_local auto threadRng = std::make_shared<Botan::AutoSeeded_RNG>();

static bool isCertificateAlert(Botan::TLS::Alert::Type type)
{
    switch (type)
    {
        case Botan::TLS::Alert::NoCertificate:
        case Botan::TLS::Alert::BadCertificate:
        case Botan::TLS::Alert::UnsupportedCertificate:
        case Botan::TLS::Alert::CertificateRevoked:
        case Botan::TLS::Alert::CertificateExpired:
        case Botan::TLS::Alert::CertificateUnknown:
        case Botan::TLS::Alert::UnknownCA:
        case Botan::TLS::Alert::CertificateUnobtainable:
        case Botan::TLS::Alert::BadCertificateStatusResponse:
        case Botan::TLS::Alert::BadCertificateHashValue:
        case Botan::TLS::Alert::CertificateRequired:
            return true;
        default:
            return false;
    }
}

static SSLError sslErrorForAlert(Botan::TLS::Alert::Type type,
                                 bool tlsConnected)
{
    if (isCertificateAlert(type))
        return SSLError::kSSLInvalidCertificate;
    return tlsConnected ? SSLError::kSSLProtocolError
                        : SSLError::kSSLHandshakeError;
}

static bool certificateMatchesHostname(const Botan::X509_Certificate &cert,
                                       std::string_view hostname)
{
// The string overload is the only matcher available across the supported
// Botan 3 releases, and unlike the newer typed overloads it also handles both
// DNS names and IP literals.
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
    const bool matches = cert.matches_dns_name(hostname);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
    return matches;
}

static std::vector<Botan::X509_Certificate> loadCertificateChain(
    Botan::DataSource &source)
{
    std::vector<Botan::X509_Certificate> chain;
    while (!source.end_of_data())
    {
        try
        {
            chain.emplace_back(source);
        }
        catch (const Botan::Decoding_Error &)
        {
            if (chain.empty() || !source.end_of_data())
                throw;
        }
    }
    return chain;
}

static bool certificateMatchesPrivateKey(
    const Botan::X509_Certificate &certificate,
    const Botan::Private_Key &privateKey)
{
    const auto certificateKey = certificate.subject_public_key();
    const auto privatePublicKey = privateKey.public_key();
    return certificateKey->algo_name() == privatePublicKey->algo_name() &&
           certificateKey->public_key_bits() ==
               privatePublicKey->public_key_bits();
}

static void validateCertificateAndKey(
    const std::vector<Botan::X509_Certificate> &certChain,
    const Botan::Private_Key *key)
{
    if (!certChain.empty() && key != nullptr &&
        !certificateMatchesPrivateKey(certChain.front(), *key))
        throw std::runtime_error(
            "The private key does not match the leaf certificate");
}

class Credentials : public Botan::Credentials_Manager
{
  private:
    struct CredentialSet
    {
        std::shared_ptr<Botan::Private_Key> key;
        std::vector<Botan::X509_Certificate> certChain;
        std::string serverName;
        bool certificateChainSelected = false;
    };

  public:
    Credentials(std::shared_ptr<Botan::Private_Key> key,
                std::vector<Botan::X509_Certificate> certChain,
                std::shared_ptr<Botan::Certificate_Store> certStore,
                ServerCertificateProvider certificateProvider)
        : configured_{std::move(key), std::move(certChain), {}, false},
          certStore_(std::move(certStore)),
          certificateProvider_(std::move(certificateProvider))
    {
        reset_for_next_handshake();
    }

    std::vector<Botan::Certificate_Store *> trusted_certificate_authorities(
        const std::string &type,
        const std::string &context) override
    {
        (void)type;
        (void)context;
        if (!certStore_)
            return {};
        return {certStore_.get()};
    }

    std::vector<Botan::X509_Certificate> find_cert_chain(
        const std::vector<std::string> &cert_key_types,
        const std::vector<Botan::AlgorithmIdentifier> &cert_signature_schemes,
        const std::vector<Botan::X509_DN> &acceptable_CAs,
        const std::string &type,
        const std::string &context) override
    {
        const auto &credentials = activeCredentials(type, context);
        if (credentials.certChain.empty())
            return {};

        const auto key_algo =
            credentials.certChain.front().subject_public_key()->algo_name();
        const auto it =
            std::find(cert_key_types.begin(), cert_key_types.end(), key_algo);
        if (!cert_key_types.empty() && it == cert_key_types.end())
            return {};
        if (!cert_signature_schemes.empty() &&
            !std::all_of(credentials.certChain.begin(),
                         credentials.certChain.end(),
                         [&cert_signature_schemes](const auto &cert) {
                             return std::find(cert_signature_schemes.begin(),
                                              cert_signature_schemes.end(),
                                              cert.signature_algorithm()) !=
                                    cert_signature_schemes.end();
                         }))
            return {};
        if (!acceptable_CAs.empty() &&
            !std::any_of(credentials.certChain.begin(),
                         credentials.certChain.end(),
                         [&acceptable_CAs](const auto &cert) {
                             return std::find(acceptable_CAs.begin(),
                                              acceptable_CAs.end(),
                                              cert.issuer_dn()) !=
                                    acceptable_CAs.end();
                         }))
            return {};
        active_.certificateChainSelected = true;
        return credentials.certChain;
    }

    std::shared_ptr<Botan::Private_Key> private_key_for(
        const Botan::X509_Certificate &cert,
        const std::string &type,
        const std::string &context) override
    {
        (void)type;
        (void)context;
        if (active_.certChain.empty() || cert != active_.certChain.front())
            return nullptr;
        return active_.key;
    }

    bool ensure_server_credentials(const std::string &serverName)
    {
        if (!serverSelected_)
            activeCredentials("tls-server", serverName);
        return active_.key != nullptr && !active_.certChain.empty();
    }

    const Botan::X509_Certificate *selected_leaf_certificate() const
    {
        if (active_.certChain.empty())
            return nullptr;
        return &active_.certChain.front();
    }

    const std::string &selected_server_name() const
    {
        return active_.serverName;
    }

    const Botan::X509_Certificate *selected_client_leaf_certificate() const
    {
        if (!active_.certificateChainSelected || active_.certChain.empty())
            return nullptr;
        return &active_.certChain.front();
    }

    void reset_for_next_handshake()
    {
        active_ = configured_;
        serverSelected_ = false;
    }

  private:
    const CredentialSet &activeCredentials(const std::string &type,
                                           const std::string &context)
    {
        if (type != "tls-server")
            return active_;

        if (serverSelected_ && active_.serverName == context)
            return active_;

        // Each Credentials instance belongs to one TLS connection. Remember
        // both successful and failed provider results so Botan can repeat its
        // lookup during a handshake without calling application code again.
        active_ = configured_;
        active_.serverName = context;
        serverSelected_ = true;
        if (!certificateProvider_)
            return active_;

        active_.key.reset();
        active_.certChain.clear();
        try
        {
            const auto certificate = certificateProvider_(context);
            if (certificate.certificatePem.empty() ||
                certificate.privateKeyPem.empty())
                return active_;

            Botan::DataSource_Memory keySource(certificate.privateKeyPem);
            auto key = Botan::PKCS8::load_key(keySource);
            Botan::DataSource_Memory certSource(certificate.certificatePem);
            auto certChain = loadCertificateChain(certSource);
            validateCertificateAndKey(certChain, key.get());
            active_.key = std::move(key);
            active_.certChain = std::move(certChain);
        }
        catch (const std::exception &e)
        {
            LOG_ERROR << "Server certificate provider failed: " << e.what();
        }
        return active_;
    }

    CredentialSet configured_;
    CredentialSet active_;
    std::shared_ptr<Botan::Certificate_Store> certStore_;
    ServerCertificateProvider certificateProvider_;
    bool serverSelected_ = false;
};

struct BotanCertificate : public Certificate
{
    BotanCertificate(const Botan::X509_Certificate &cert) : cert_(cert)
    {
    }

    std::string sha1Fingerprint() const override
    {
        return cert_.fingerprint("SHA-1");
    }

    std::string sha256Fingerprint() const override
    {
        return cert_.fingerprint("SHA-256");
    }

    std::string pem() const override
    {
        return cert_.PEM_encode();
    }
    Botan::X509_Certificate cert_;
};

CertificatePtr trantor::Certificate::fromPem(const std::string &pem)
{
    try
    {
        Botan::DataSource_Memory source(pem);
        return std::make_shared<BotanCertificate>(
            Botan::X509_Certificate(source));
    }
    catch (const Botan::Exception &)
    {
        return nullptr;
    }
}

namespace trantor
{
struct SSLContext
{
    std::shared_ptr<Botan::Private_Key> key;
    std::vector<Botan::X509_Certificate> certChain;
    std::shared_ptr<Botan::Certificate_Store> certStore;
    std::shared_ptr<Botan::TLS::Session_Manager_In_Memory> sessionManager;
    ServerCertificateProvider certificateProvider;
    std::vector<std::string> alpnProtocols;
    std::string hostname;
    bool isServer = false;
    bool validate = true;
    bool allowBrokenChain = false;
    bool requestClientCert = false;
    bool requireClientCert = false;
};
}  // namespace trantor

class TrantorPolicy : public Botan::TLS::Policy
{
  public:
    TrantorPolicy(bool requestClientCert, bool requireClientCert)
        : requestClientCert_(requestClientCert),
          requireClientCert_(requireClientCert)
    {
    }

  private:
    bool require_cert_revocation_info() const override
    {
        return false;
    }

    bool require_client_certificate_authentication() const override
    {
        return requireClientCert_;
    }

    bool request_client_certificate_authentication() const override
    {
        return requestClientCert_;
    }

    const bool requestClientCert_;
    const bool requireClientCert_;
};

struct BotanTLSProvider : public TLSProvider,
                          public NonCopyable,
                          public Botan::TLS::Callbacks
{
    // Botan callbacks only collect output, metadata and events. Transport I/O
    // and Trantor callbacks run after the active Botan operation unwinds.
  public:
    BotanTLSProvider(TcpConnection *conn,
                     TLSPolicyPtr policy,
                     SSLContextPtr ctx)
        : TLSProvider(conn, std::move(policy), std::move(ctx))
    {
        validationPolicy_ =
            std::make_shared<TrantorPolicy>(contextPtr_->requestClientCert,
                                            contextPtr_->requireClientCert);
    }

    void recvData(MsgBuffer *buffer) override
    {
        LOG_TRACE << "Low level connection received " << buffer->readableBytes()
                  << " bytes.";
        if (failed_ || !channel_)
        {
            if (!failed_)
                fail("receiving TLS data",
                     "the TLS channel is not initialized",
                     SSLError::kSSLHandshakeError);
            buffer->retrieveAll();
            dispatchEvents();
            return;
        }

        drive("receiving TLS data", [this, buffer]() {
            channel_->received_data(reinterpret_cast<const uint8_t *>(
                                        buffer->peek()),
                                    buffer->readableBytes());
        });
        buffer->retrieveAll();
        dispatchEvents();
    }

    ssize_t sendData(const char *ptr, size_t size) override
    {
        if (failed_ || !channel_ || !channel_->is_active())
        {
            errno = failed_ ? EPIPE : EAGAIN;
            return failed_ ? -1 : 0;
        }
        if (getBufferedData().readableBytes() != 0)
        {
            errno = EAGAIN;
            return 0;
        }

        constexpr size_t maxSend = 64 * 1024;
        size_t accepted = 0;
        while (accepted < size && getBufferedData().readableBytes() == 0)
        {
            const auto chunkLen = (std::min)(size - accepted, maxSend);
            if (!drive("sending TLS data", [this, ptr, accepted, chunkLen]() {
                    channel_->send(reinterpret_cast<const uint8_t *>(ptr) +
                                       accepted,
                                   chunkLen);
                }))
            {
                dispatchEvents();
                return -1;
            }
            accepted += chunkLen;
        }
        dispatchEvents();
        return static_cast<ssize_t>(accepted);
    }

    void close() override
    {
        if (failed_ || !channel_ || !channel_->is_active())
            return;
        drive("closing the TLS channel", [this]() { channel_->close(); });
        dispatchEvents();
    }

    void startEncryption() override
    {
        if (channel_ || failed_)
            return;

        credsPtr_ =
            std::make_shared<Credentials>(contextPtr_->key,
                                          contextPtr_->certChain,
                                          contextPtr_->certStore,
                                          contextPtr_->certificateProvider);

        // channel_ is a strict child of this provider, so its callbacks cannot
        // outlive us. A genuinely owning shared_ptr would create a cycle:
        // provider -> channel -> callbacks -> provider.
        auto callbacks = std::shared_ptr<Botan::TLS::Callbacks>(
            this, [](Botan::TLS::Callbacks *) {});
        drive("starting TLS",
              [this, callbacks = std::move(callbacks)]() mutable {
                  if (contextPtr_->isServer)
                      channel_ = std::make_unique<Botan::TLS::Server>(
                          std::move(callbacks),
                          contextPtr_->sessionManager,
                          credsPtr_,
                          validationPolicy_,
                          threadRng);
                  else
                  {
                      channel_ = std::make_unique<Botan::TLS::Client>(
                          std::move(callbacks),
                          contextPtr_->sessionManager,
                          credsPtr_,
                          validationPolicy_,
                          threadRng,
                          Botan::TLS::Server_Information(
                              contextPtr_->hostname,
                              conn_->peerAddr().toPort()),
                          Botan::TLS::Protocol_Version::latest_tls_version(),
                          contextPtr_->alpnProtocols);
                      setSniName(contextPtr_->hostname);
                  }
              });
        dispatchEvents();
    }

    bool sendBufferedData() override
    {
        const auto drained = flushOutput();
        dispatchEvents();
        return drained;
    }

    ~BotanTLSProvider() override = default;

    void tls_emit_data(std::span<const uint8_t> data) override
    {
        appendToWriteBuffer(reinterpret_cast<const char *>(data.data()),
                            data.size_bytes());
    }

    void tls_record_received(uint64_t seq_no,
                             std::span<const uint8_t> data) override
    {
        (void)seq_no;
        recvBuffer_.append(reinterpret_cast<const char *>(data.data()),
                           data.size_bytes());
        plaintextPending_ = true;
    }

    std::string tls_server_choose_app_protocol(
        const std::vector<std::string> &client_protos) override
    {
        assert(contextPtr_->isServer);
        for (const auto &proto : contextPtr_->alpnProtocols)
        {
            if (std::find(client_protos.begin(), client_protos.end(), proto) !=
                client_protos.end())
                return proto;
        }
        if (!contextPtr_->alpnProtocols.empty() && !client_protos.empty())
            throw Botan::TLS::TLS_Exception(
                Botan::TLS::Alert::NoApplicationProtocol,
                "No overlapping ALPN protocols between client and server");
        return "";
    }

    void tls_alert(Botan::TLS::Alert alert) override
    {
        if (alert.type() == Botan::TLS::Alert::CloseNotify)
        {
            closePending_ = true;
            return;
        }
        if (!alert.is_fatal())
        {
            LOG_TRACE << "Non-fatal TLS alert received: "
                      << alert.type_string();
            return;
        }

        fail("processing a TLS alert",
             alert.type_string().c_str(),
             sslErrorForAlert(alert.type(), channel_ && channel_->is_active()));
    }

    void tls_session_activated() override
    {
        LOG_TRACE << "tls_session_activated";
        setApplicationProtocol(channel_->application_protocol());
        if (!contextPtr_->isServer)
        {
            const auto *localCertificate =
                credsPtr_->selected_client_leaf_certificate();
            setLocalCertificate(
                localCertificate == nullptr
                    ? nullptr
                    : std::make_shared<BotanCertificate>(*localCertificate));
            credsPtr_->reset_for_next_handshake();
        }
        handshakePending_ = true;
    }

    void tls_session_established(
        const Botan::TLS::Session_Summary &session) override
    {
        if (!session.peer_certs().empty())
            setPeerCertificate(std::make_shared<BotanCertificate>(
                session.peer_certs().front()));

        if (contextPtr_->isServer)
        {
            const auto serverName = session.server_info().hostname();
            // Resumed handshakes may not request a certificate from
            // Credentials. Select once here as well so metadata remains
            // complete and a provider can reject a resumed connection.
            if (!credsPtr_->ensure_server_credentials(serverName))
                throw Botan::TLS::TLS_Exception(
                    Botan::TLS::Alert::HandshakeFailure,
                    "No server credentials are available for this connection");
            updateServerMetadata();
            credsPtr_->reset_for_next_handshake();
        }
    }

    void updateServerMetadata()
    {
        setSniName(credsPtr_->selected_server_name());
        const auto *localCertificate = credsPtr_->selected_leaf_certificate();
        if (localCertificate != nullptr)
            setLocalCertificate(
                std::make_shared<BotanCertificate>(*localCertificate));
    }

    void tls_verify_cert_chain(
        const std::vector<Botan::X509_Certificate> &certs,
        const std::vector<std::optional<Botan::OCSP::Response>> &ocsp,
        const std::vector<Botan::Certificate_Store *> &trusted_roots,
        Botan::Usage_Type usage,
        std::string_view hostname,
        const Botan::TLS::Policy &policy) override
    {
        if (!contextPtr_->isServer)
            setSniName(std::string(hostname));
        if (contextPtr_->validate)
        {
            if (certs.empty())
                throw Botan::TLS::TLS_Exception(
                    Botan::TLS::Alert::NoCertificate,
                    "Certificate validation failed: no certificate");
            if (contextPtr_->allowBrokenChain)
            {
                const auto &cert = certs[0];
                const auto now = std::chrono::system_clock::now();
                if (now < cert.not_before().to_std_timepoint() ||
                    now > cert.not_after().to_std_timepoint())
                {
                    throw Botan::TLS::TLS_Exception(
                        Botan::TLS::Alert::CertificateExpired,
                        "Certificate validation failed: certificate is not "
                        "currently valid");
                }
                if (!contextPtr_->isServer && !hostname.empty() &&
                    !certificateMatchesHostname(cert, hostname))
                {
                    throw Botan::TLS::TLS_Exception(
                        Botan::TLS::Alert::BadCertificate,
                        "Certificate validation failed: hostname mismatch");
                }
            }
            else
                Botan::TLS::Callbacks::tls_verify_cert_chain(
                    certs, ocsp, trusted_roots, usage, hostname, policy);
        }

        if (!certs.empty())
            setPeerCertificate(std::make_shared<BotanCertificate>(certs[0]));
    }

  private:
    template <typename Operation>
    bool drive(const char *description, Operation &&operation)
    {
        const bool wasActive = channel_ && channel_->is_active();
        try
        {
            operation();
        }
        catch (const Botan::TLS::TLS_Exception &e)
        {
            fail(description, e.what(), sslErrorForAlert(e.type(), wasActive));
        }
        catch (const Botan::Exception &e)
        {
            fail(description,
                 e.what(),
                 wasActive ? SSLError::kSSLProtocolError
                           : SSLError::kSSLHandshakeError);
        }
        catch (const std::exception &e)
        {
            fail(description,
                 e.what(),
                 wasActive ? SSLError::kSSLProtocolError
                           : SSLError::kSSLHandshakeError);
        }
        flushOutput();
        return !failed_;
    }

    void fail(const char *operation, const char *message, SSLError error)
    {
        if (failed_)
            return;
        LOG_ERROR << "Botan failed while " << operation << ": " << message;
        failed_ = true;
        pendingError_ = error;
        closePending_ = false;
        handshakePending_ = false;
        plaintextPending_ = false;
    }

    bool flushOutput()
    {
        auto &output = getBufferedData();
        if (output.readableBytes() == 0)
            return true;
        assert(writeCallback_ != nullptr);
        const auto n =
            writeCallback_(conn_, output.peek(), output.readableBytes());
        if (n < 0)
        {
            fail("flushing TLS data",
                 "the transport write failed",
                 SSLError::kSSLProtocolError);
            return false;
        }
        output.retrieve(static_cast<size_t>(n));
        return output.readableBytes() == 0;
    }

    void dispatchEvents()
    {
        if (dispatchingEvents_)
            return;

        auto guard = shared_from_this();
        DispatchGuard dispatchGuard(dispatchingEvents_);
        while (pendingError_ || handshakePending_ || plaintextPending_ ||
               closePending_)
        {
            if (pendingError_)
            {
                const auto error = *pendingError_;
                pendingError_.reset();
                if (errorCallback_)
                    errorCallback_(conn_, error);
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
            LOG_TRACE << "TLS close notify received";
            if (closeCallback_)
                closeCallback_(conn_);
        }
    }

    struct DispatchGuard
    {
        explicit DispatchGuard(bool &dispatching) : dispatching_(dispatching)
        {
            dispatching_ = true;
        }

        ~DispatchGuard()
        {
            dispatching_ = false;
        }

        bool &dispatching_;
    };

    std::shared_ptr<TrantorPolicy> validationPolicy_;
    std::shared_ptr<Credentials> credsPtr_;
    std::unique_ptr<Botan::TLS::Channel> channel_;
    std::optional<SSLError> pendingError_;
    bool failed_ = false;
    bool closePending_ = false;
    bool handshakePending_ = false;
    bool plaintextPending_ = false;
    bool dispatchingEvents_ = false;
};

std::shared_ptr<TLSProvider> trantor::newTLSProvider(TcpConnection *conn,
                                                     TLSPolicyPtr policy,
                                                     SSLContextPtr ctx)
{
    return std::make_shared<BotanTLSProvider>(conn,
                                              std::move(policy),
                                              std::move(ctx));
}

SSLContextPtr trantor::newSSLContext(const TLSPolicy &policy, bool server)
{
    auto ctx = std::make_shared<SSLContext>();
    ctx->isServer = server;
    ctx->certificateProvider = policy.getServerCertificateProvider();
    ctx->alpnProtocols = policy.getAlpnProtocols();
    ctx->hostname = policy.getHostname();
    ctx->validate = policy.getValidate();
    ctx->allowBrokenChain = policy.getAllowBrokenChain();
    auto sessionRng = std::make_shared<Botan::AutoSeeded_RNG>();
    // This deliberately provides stateful TLS 1.2 resumption only. Botan's
    // in-memory manager does not issue the server tickets needed for TLS 1.3
    // resumption; changing that requires an explicit ticket-key policy.
    ctx->sessionManager =
        std::make_shared<Botan::TLS::Session_Manager_In_Memory>(sessionRng);

    if (!policy.getCertificatePem().empty())
    {
        Botan::DataSource_Memory keySource(policy.getPrivateKeyPem());
        ctx->key = Botan::PKCS8::load_key(keySource);
        Botan::DataSource_Memory certSource(policy.getCertificatePem());
        ctx->certChain = loadCertificateChain(certSource);
    }
    else
    {
        if (!policy.getKeyPath().empty())
        {
            Botan::DataSource_Stream keySource(policy.getKeyPath());
            ctx->key = Botan::PKCS8::load_key(keySource);
        }

        if (!policy.getCertPath().empty())
        {
            Botan::DataSource_Stream certSource(policy.getCertPath());
            ctx->certChain = loadCertificateChain(certSource);
        }
    }

    validateCertificateAndKey(ctx->certChain, ctx->key.get());

    if (policy.getValidate())
    {
        if (!policy.getCaPath().empty())
        {
            ctx->certStore =
                std::make_shared<Botan::Certificate_Store_In_Memory>(
                    policy.getCaPath());
            if (ctx->certStore->all_subjects().empty())
                throw std::runtime_error(
                    "The configured CA path contains no certificates");
        }
        else if (policy.getUseSystemCertStore())
        {
            static auto systemCertStore =
                std::make_shared<Botan::System_Certificate_Store>();
            ctx->certStore = systemCertStore;
        }
    }
    if (server && policy.getPeerCertificateRequest())
    {
        ctx->requestClientCert = true;
        ctx->requireClientCert = policy.getRequirePeerCertificate();
    }
    else if (server && policy.getValidate() && !policy.getCaPath().empty())
    {
        // Preserve the legacy CA-path behaviour for callers that do not use
        // the explicit request API.
        ctx->requestClientCert = true;
        ctx->requireClientCert = true;
    }

    if (policy.getUseOldTLS())
        LOG_WARN << "TLSPolicy enables old TLS, but Botan does not support "
                    "TLS/SSL below TLS 1.2. Ignoring this option.";
    if (!policy.getConfCmds().empty())
        LOG_WARN << "Botan does not support sslConfCmds; ignoring them.";
    return ctx;
}
