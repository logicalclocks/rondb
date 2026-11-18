#include <trantor/net/inner/BufferNode.h>
namespace trantor
{
static const size_t kMaxSendFileBufferSize = 16 * 1024;
class StreamBufferNode : public BufferNode
{
  public:
    StreamBufferNode(std::function<std::size_t(char *, std::size_t)> &&callback)
        : streamCallback_(std::move(callback))
    {
    }
    bool isStream() const override
    {
        return true;
    }
    void getData(const char *&data, size_t &len) override
    {
        if (msgBuffer_.readableBytes() == 0)
        {
            msgBuffer_.ensureWritableBytes(kMaxSendFileBufferSize);
            auto n = streamCallback_(msgBuffer_.beginWrite(),
                                     msgBuffer_.writableBytes());
            if (n > 0)
            {
                msgBuffer_.hasWritten(n);
            }
            else
            {
                isDone_ = true;
            }
        }
        data = msgBuffer_.peek();
        len = msgBuffer_.readableBytes();
    }
    void retrieve(size_t len) override
    {
        msgBuffer_.retrieve(len);
#ifndef NDEBUG
        dataWritten_ += len;
        LOG_TRACE << "send stream in loop: bytes written: " << dataWritten_
                  << " / total bytes written: " << dataWritten_;
#endif
    }
    long long remainingBytes() const override
    {
        if (isDone_)
            return 0;
        return 1;
    }
    ~StreamBufferNode() override
    {
        if (streamCallback_)
            streamCallback_(nullptr, 0);  // cleanup callback internals
    }

  private:
    std::function<std::size_t(char *, std::size_t)> streamCallback_;
#ifndef NDEBUG  // defined by CMake for release build
    std::size_t dataWritten_{0};
#endif
    MsgBuffer msgBuffer_;
};
BufferNodePtr BufferNode::newStreamBufferNode(StreamCallback &&callback)
{
    return std::make_shared<StreamBufferNode>(std::move(callback));
}
}  // namespace trantor