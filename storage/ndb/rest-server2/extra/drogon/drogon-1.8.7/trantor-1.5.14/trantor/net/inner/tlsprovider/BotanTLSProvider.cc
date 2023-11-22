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
#include <botan/pkix_types.h>
#include <botan/certstor_flatfile.h>
#include <botan/x509path.h>
#include <botan/tls_session_manager_memory.h>
#include <memory>

using namespace trantor;
using namespace std::placeholders;

static std::once_flag sessionManagerInitFlag;
static std::shared_ptr<Botan::AutoSeeded_RNG> sessionManagerRng;
static std::shared_ptr<Botan::TLS::Session_Manager_In_Memory> sessionManager;
static thread_local std::shared_ptr<Botan::AutoSeeded_RNG> rng;
static Botan::System_Certificate_Store certStore;

using namespace trantor;

class Credentials : public Botan::Credentials_Manager
{
  public:
    Credentials(std::shared_ptr<Botan::Private_Key> key,
                Botan::X509_Certificate *cert,
                Botan::Certificate_Store *certStore)
        : certStore_(certStore), cert_(cert), key_(key)
    {
    }
    std::vector<Botan::Certificate_Store *> trusted_certificate_authorities(
        const std::string &type,
        const std::string &context) override
    {
        (void)type;
        (void)context;
        if (certStore_ == nullptr)
            return {};
        return {certStore_};
    }

    std::vector<Botan::X509_Certificate> find_cert_chain(
        const std::vector<std::string> &cert_key_types,
        const std::vector<Botan::AlgorithmIdentifier> &cert_signature_schemes,
        const std::vector<Botan::X509_DN> &acceptable_CAs,
        const std::string &type,
        const std::string &context) override
    {
        (void)type;
        (void)context;
        (void)cert_signature_schemes;
        (void)acceptable_CAs;
        if (cert_ == nullptr)
            return {};

        auto key_algo =
            cert_->subject_public_key_algo().oid().to_formatted_string();
        auto it =
            std::find(cert_key_types.begin(), cert_key_types.end(), key_algo);
        if (it == cert_key_types.end())
            return {};
        return {*cert_};
    }

    std::shared_ptr<Botan::Private_Key> private_key_for(
        const Botan::X509_Certificate &cert,
        const std::string &type,
        const std::string &context) override
    {
        (void)cert;
        (void)type;
        (void)context;
        return key_;
    }
    Botan::Certificate_Store *certStore_ = nullptr;
    Botan::X509_Certificate *cert_ = nullptr;
    std::shared_ptr<Botan::Private_Key> key_ = nullptr;
};

struct BotanCertificate : public Certificate
{
    BotanCertificate(const Botan::X509_Certificate &cert) : cert_(cert)
    {
    }

    virtual std::string sha1Fingerprint() const override
    {
        return cert_.fingerprint("SHA-1");
    }

    virtual std::string sha256Fingerprint() const override
    {
        return cert_.fingerprint("SHA-256");
    }

    virtual std::string pem() const override
    {
        return cert_.PEM_encode();
    }
    Botan::X509_Certificate cert_;
};

namespace trantor
{
struct SSLContext
{
    std::shared_ptr<Botan::Private_Key> key;
    std::unique_ptr<Botan::X509_Certificate> cert;
    std::shared_ptr<Botan::Certificate_Store> certStore;
    bool isServer = false;
    bool requireClientCert = false;
};
}  // namespace trantor

class TrantorPolicy : public Botan::TLS::Policy
{
    virtual bool require_cert_revocation_info() const override
    {
        return false;
    }

    virtual bool require_client_certificate_authentication() const override
    {
        return requireClientCert_;
    }

  public:
    bool requireClientCert_ = false;
};

struct BotanTLSProvider : public TLSProvider,
                          public NonCopyable,
                          public Botan::TLS::Callbacks,
                          public std::enable_shared_from_this<BotanTLSProvider>
{
  public:
    BotanTLSProvider(TcpConnection *conn,
                     TLSPolicyPtr policy,
                     SSLContextPtr ctx)
        : TLSProvider(conn, std::move(policy), std::move(ctx))
    {
        validationPolicy_ = std::make_shared<TrantorPolicy>();
    }

    virtual void recvData(MsgBuffer *buffer) override
    {
        LOG_TRACE << "Low level connection received " << buffer->readableBytes()
                  << " bytes.";
        try
        {
            assert(channel_ != nullptr);
            channel_->received_data((const uint8_t *)buffer->peek(),
                                    buffer->readableBytes());
        }
        catch (const Botan::TLS::TLS_Exception &e)
        {
            LOG_ERROR << "Unexpected TLS Exception: " << e.what();
            conn_->shutdown();

            if (tlsConnected_ == false)
            {
                if (e.type() == Botan::TLS::Alert::BadCertificate)
                    handleSSLError(SSLError::kSSLInvalidCertificate);
                else
                    handleSSLError(SSLError::kSSLHandshakeError);
            }
            else
                handleSSLError(SSLError::kSSLProtocolError);
        }
        catch (const Botan::Exception &e)
        {
            LOG_ERROR << "Unexpected Botan Exception: " << e.what();
            conn_->shutdown();
            if (tlsConnected_ == false)
                handleSSLError(SSLError::kSSLHandshakeError);
            else
                handleSSLError(SSLError::kSSLProtocolError);
        }
        catch (const std::exception &e)
        {
            LOG_ERROR << "Unexpected Generic Exception: " << e.what();
            conn_->shutdown();
            if (tlsConnected_ == false)
                handleSSLError(SSLError::kSSLHandshakeError);
            else
                handleSSLError(SSLError::kSSLProtocolError);
        }
        buffer->retrieveAll();
    }

    virtual ssize_t sendData(const char *ptr, size_t size) override
    {
        if (getBufferedData().readableBytes() != 0)
        {
            errno = EAGAIN;
            return -1;
        }

        // Limit the size of the data we send in one go to avoid holding massive
        // buffers in memory.
        constexpr size_t maxSend = 64 * 1024;
        if (size > maxSend)
            size = maxSend;
        channel_->send((const uint8_t *)ptr, size);

        // HACK: Botan doesn't provide a way to know how much raw data has been
        // written to the underlying transport. So we have to assume that all
        // data has been written. And cache the unwritten data in writeBuffer_.
        // Then "fake" the consumed size in sendData() to make the caller think
        // that all data has been written. Then return -1 if the underlying
        // socket is not writable at all (i.e. write is all or nothing)
        if (lastWriteSize_ == -1)
            return -1;
        return size;
    }

    virtual void close() override
    {
        if (channel_ && channel_->is_active())
            channel_->close();
    }

    virtual void startEncryption() override
    {
        credsPtr_ = std::make_shared<Credentials>(contextPtr_->key,
                                                  contextPtr_->cert.get(),
                                                  contextPtr_->certStore.get());
        if (policyPtr_->getConfCmds().empty() == false)
            LOG_WARN << "BotanTLSConnectionImpl does not support sslConfCmds.";

        // initialize rng and session manager if we haven't already
        std::call_once(sessionManagerInitFlag, []() {
            sessionManagerRng = std::make_shared<Botan::AutoSeeded_RNG>();
            sessionManager =
                std::make_shared<Botan::TLS::Session_Manager_In_Memory>(
                    sessionManagerRng);
        });
        if (rng == nullptr)
            rng = std::make_shared<Botan::AutoSeeded_RNG>();
        if (contextPtr_->isServer)
        {
            // TODO: Need a more scalable way to manage session validation rules
            validationPolicy_->requireClientCert_ =
                contextPtr_->requireClientCert;
            channel_ = std::make_unique<Botan::TLS::Server>(shared_from_this(),
                                                            sessionManager,
                                                            credsPtr_,
                                                            validationPolicy_,
                                                            rng);
        }
        else
        {
            validationPolicy_->requireClientCert_ =
                contextPtr_->requireClientCert;
            // technically Botan2 does support TLS 1.0 and 1.1, but Botan3 does
            // not. So we just disable them to keep compatibility.
            if (policyPtr_->getUseOldTLS())
                LOG_WARN << "Old TLS not supported by Botan (only >= TLS 1.2)";
            channel_ = std::make_unique<Botan::TLS::Client>(
                shared_from_this(),
                sessionManager,
                credsPtr_,
                validationPolicy_,
                rng,
                Botan::TLS::Server_Information(policyPtr_->getHostname(),
                                               conn_->peerAddr().toPort()),
                Botan::TLS::Protocol_Version::TLS_V12,
                policyPtr_->getAlpnProtocols());
            setSniName(policyPtr_->getHostname());
        }
    }

    void handleSSLError(SSLError err)
    {
        if (!errorCallback_)
            return;
        loop_->queueInLoop([this, err]() { errorCallback_(conn_, err); });
    }

    virtual ~BotanTLSProvider() override = default;

    void tls_emit_data(std::span<const uint8_t> data) override
    {
        auto n = writeCallback_(conn_, data.data(), data.size_bytes());
        lastWriteSize_ = n;

        // store the unsent data and send it later
        if (n == ssize_t(data.size_bytes()))
            return;
        if (n == -1)
            n = 0;
        appendToWriteBuffer((const char *)data.data() + n,
                            data.size_bytes() - n);
    }

    void tls_record_received(uint64_t seq_no,
                             std::span<const uint8_t> data) override
    {
        (void)seq_no;
        recvBuffer_.append((const char *)data.data(), data.size_bytes());
        if (messageCallback_)
            messageCallback_(conn_, &recvBuffer_);
    }

    void tls_alert(Botan::TLS::Alert alert) override
    {
        if (alert.type() == Botan::TLS::Alert::CloseNotify)
        {
            LOG_TRACE << "TLS close notify received";
            if (closeCallback_)
                closeCallback_(conn_);
        }
        else
        {
            if (errorCallback_)
                errorCallback_(conn_, SSLError::kSSLProtocolError);
        }
    }

    void tls_session_established(
        const Botan::TLS::Session_Summary &session) override
    {
        (void)session;
        LOG_TRACE << "tls_session_established";
        tlsConnected_ = true;
        loop_->queueInLoop([this]() {
            setApplicationProtocol(channel_->application_protocol());
            if (handshakeCallback_)
                handshakeCallback_(conn_);
        });
    }

    void tls_verify_cert_chain(
        const std::vector<Botan::X509_Certificate> &certs,
        const std::vector<std::optional<Botan::OCSP::Response>> &ocsp,
        const std::vector<Botan::Certificate_Store *> &trusted_roots,
        Botan::Usage_Type usage,
        const std::string &hostname,
        const Botan::TLS::Policy &policy)
    {
        setSniName(hostname);
        if (policyPtr_->getValidate() && !policyPtr_->getAllowBrokenChain())
            Botan::TLS::Callbacks::tls_verify_cert_chain(
                certs, ocsp, trusted_roots, usage, hostname, policy);
        else if (policyPtr_->getValidate())
        {
            if (certs.size() == 0)
                throw Botan::TLS::TLS_Exception(
                    Botan::TLS::Alert::NoCertificate,
                    "Certificate validation failed: no certificate");
            // handle self-signed certificate
            std::vector<Botan::X509_Certificate> selfSigned = {certs[0]};

            Botan::Path_Validation_Restrictions restrictions(
                false,  // require revocation
                validationPolicy_->minimum_signature_strength());

            auto now = std::chrono::system_clock::now();
            const auto status = Botan::PKIX::check_chain(
                selfSigned, now, hostname, usage, restrictions);

            const auto result = Botan::PKIX::overall_status(status);

            if (result != Botan::Certificate_Status_Code::OK)
                throw Botan::TLS::TLS_Exception(
                    Botan::TLS::Alert::BadCertificate,
                    std::string("Certificate validation failed: ") +
                        Botan::to_string(result));
        }
    }

    std::shared_ptr<TrantorPolicy> validationPolicy_;
    std::shared_ptr<Botan::Credentials_Manager> credsPtr_;
    std::unique_ptr<Botan::TLS::Channel> channel_;
    bool tlsConnected_ = false;
    ssize_t lastWriteSize_ = 0;
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
    if (!policy.getKeyPath().empty())
    {
        Botan::DataSource_Stream in(policy.getKeyPath());
        ctx->key = Botan::PKCS8::load_key(in);
    }

    if (!policy.getCertPath().empty())
    {
        ctx->cert =
            std::make_unique<Botan::X509_Certificate>(policy.getCertPath());
    }

    if (policy.getValidate() && policy.getAllowBrokenChain())
    {
        if (!policy.getCaPath().empty())
        {
            ctx->certStore =
                std::make_shared<Botan::Flatfile_Certificate_Store>(
                    policy.getCaPath());
            if (server)
                ctx->requireClientCert = true;
        }
        else if (policy.getUseSystemCertStore())
        {
            static auto systemCertStore =
                std::make_shared<Botan::System_Certificate_Store>();
            ctx->certStore = systemCertStore;
        }
    }

    if (policy.getUseOldTLS())
        LOG_WARN << "SSLPloicy have set useOldTLS to true. BUt Botan does not "
                    "support TLS/SSL below TLS 1.2. Ignoreing this option.";
    return ctx;
}
