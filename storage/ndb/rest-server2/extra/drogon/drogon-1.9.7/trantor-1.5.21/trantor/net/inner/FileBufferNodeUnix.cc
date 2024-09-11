#include <trantor/net/inner/BufferNode.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <algorithm>

namespace trantor
{
static const size_t kMaxSendFileBufferSize = 16 * 1024;
class FileBufferNode : public BufferNode
{
  public:
    FileBufferNode(const char *fileName, long long offset, long long length)
    {
        assert(offset >= 0);
        if (offset < 0)
        {
            LOG_ERROR << "offset must be greater than or equal to 0";
            isDone_ = true;
            return;
        }
        sendFd_ = open(fileName, O_RDONLY);

        if (sendFd_ < 0)
        {
            LOG_SYSERR << fileName << " open error";
            isDone_ = true;
            return;
        }
        struct stat filestat;
        if (stat(fileName, &filestat) < 0)
        {
            LOG_SYSERR << fileName << " stat error";
            close(sendFd_);
            sendFd_ = -1;
            isDone_ = true;
            return;
        }
        if (length == 0)
        {
            if (offset >= filestat.st_size)
            {
                LOG_ERROR << "The file size is " << filestat.st_size
                          << " bytes, but the offset is " << offset
                          << " bytes and the length is " << length << " bytes";
                close(sendFd_);
                sendFd_ = -1;
                isDone_ = true;
                return;
            }
            fileBytesToSend_ = filestat.st_size - offset;
        }
        else
        {
            if (length > filestat.st_size - offset)
            {
                LOG_ERROR << "The file size is " << filestat.st_size
                          << " bytes, but the offset is " << offset
                          << " bytes and the length is " << length << " bytes";
                close(sendFd_);
                sendFd_ = -1;
                isDone_ = true;
                return;
            }
            fileBytesToSend_ = length;
        }
        lseek(sendFd_, offset, SEEK_SET);
    }
    bool isFile() const override
    {
        return true;
    }
    int getFd() const override
    {
        return sendFd_;
    }
    void getData(const char *&data, size_t &len) override
    {
        if (msgBufferPtr_ == nullptr)
        {
            msgBufferPtr_ = std::make_unique<MsgBuffer>(
                (std::min)(kMaxSendFileBufferSize,
                           static_cast<size_t>(fileBytesToSend_)));
        }
        if (msgBufferPtr_->readableBytes() == 0 && fileBytesToSend_ > 0 &&
            sendFd_ >= 0)
        {
            msgBufferPtr_->ensureWritableBytes(
                (std::min)(kMaxSendFileBufferSize,
                           static_cast<size_t>(fileBytesToSend_)));
            auto n = read(sendFd_,
                          msgBufferPtr_->beginWrite(),
                          msgBufferPtr_->writableBytes());
            if (n > 0)
            {
                msgBufferPtr_->hasWritten(n);
            }
            else if (n == 0)
            {
                LOG_TRACE << "Read the end of file.";
            }
            else
            {
                LOG_SYSERR << "FileBufferNode::getData()";
            }
        }
        data = msgBufferPtr_->peek();
        len = msgBufferPtr_->readableBytes();
    }
    void retrieve(size_t len) override
    {
        if (msgBufferPtr_)
        {
            msgBufferPtr_->retrieve(len);
        }
        fileBytesToSend_ -= static_cast<long long>(len);
        if (fileBytesToSend_ < 0)
            fileBytesToSend_ = 0;
    }
    long long remainingBytes() const override
    {
        if (isDone_)
            return 0;
        return fileBytesToSend_;
    }
    ~FileBufferNode() override
    {
        if (sendFd_ >= 0)
        {
            close(sendFd_);
        }
    }
    bool available() const override
    {
        return sendFd_ >= 0;
    }

  private:
    int sendFd_{-1};
    long long fileBytesToSend_{0};
    std::unique_ptr<MsgBuffer> msgBufferPtr_;
};

BufferNodePtr BufferNode::newFileBufferNode(const char *fileName,
                                            long long offset,
                                            long long length)
{
    return std::make_shared<FileBufferNode>(fileName, offset, length);
}
}  // namespace trantor