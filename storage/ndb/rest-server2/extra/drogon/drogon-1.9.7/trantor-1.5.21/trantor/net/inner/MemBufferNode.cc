#include <trantor/net/inner/BufferNode.h>
namespace trantor
{
class MemBufferNode : public BufferNode
{
  public:
    MemBufferNode() = default;

    void getData(const char *&data, size_t &len) override
    {
        data = buffer_.peek();
        len = buffer_.readableBytes();
    }
    void retrieve(size_t len) override
    {
        buffer_.retrieve(len);
    }
    long long remainingBytes() const override
    {
        if (isDone_)
            return 0;
        return static_cast<long long>(buffer_.readableBytes());
    }
    void append(const char *data, size_t len) override
    {
        buffer_.append(data, len);
    }

  private:
    trantor::MsgBuffer buffer_;
};
BufferNodePtr BufferNode::newMemBufferNode()
{
    return std::make_shared<MemBufferNode>();
}
}  // namespace trantor
