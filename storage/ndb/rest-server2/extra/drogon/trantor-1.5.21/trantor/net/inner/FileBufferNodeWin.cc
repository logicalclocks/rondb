#include <trantor/net/inner/BufferNode.h>
#include <windows.h>
#include <fileapi.h>
#if defined(WINAPI_FAMILY) && (WINAPI_FAMILY == WINAPI_FAMILY_APP)
#define UWP 1
#else
#define UWP 0
#endif

namespace trantor
{
static const size_t kMaxSendFileBufferSize = 16 * 1024;
class FileBufferNode : public BufferNode
{
  public:
    FileBufferNode(const wchar_t *fileName, long long offset, long long length)
    {
#if UWP
        sendHandle_ = CreateFile2(
            fileName, GENERIC_READ, FILE_SHARE_READ, OPEN_EXISTING, nullptr);

#else
        sendHandle_ = CreateFileW(fileName,
                                  GENERIC_READ,
                                  FILE_SHARE_READ,
                                  nullptr,
                                  OPEN_EXISTING,
                                  FILE_ATTRIBUTE_NORMAL,
                                  nullptr);
#endif
        if (sendHandle_ == INVALID_HANDLE_VALUE)
        {
            LOG_SYSERR << fileName << " open error";
            isDone_ = true;
            return;
        }
        LARGE_INTEGER fileSize;
        if (!GetFileSizeEx(sendHandle_, &fileSize))
        {
            LOG_SYSERR << fileName << " stat error";
            CloseHandle(sendHandle_);
            sendHandle_ = INVALID_HANDLE_VALUE;
            isDone_ = true;
            return;
        }

        if (length == 0)
        {
            if (offset >= fileSize.QuadPart)
            {
                LOG_ERROR << "The file size is " << fileSize.QuadPart
                          << " bytes, but the offset is " << offset
                          << " bytes and the length is " << length << " bytes";
                CloseHandle(sendHandle_);
                sendHandle_ = INVALID_HANDLE_VALUE;
                isDone_ = true;
                return;
            }
            fileBytesToSend_ = fileSize.QuadPart - offset;
        }
        else
        {
            if (length + offset > fileSize.QuadPart)
            {
                LOG_ERROR << "The file size is " << fileSize.QuadPart
                          << " bytes, but the offset is " << offset
                          << " bytes and the length is " << length << " bytes";
                CloseHandle(sendHandle_);
                sendHandle_ = INVALID_HANDLE_VALUE;
                isDone_ = true;
                return;
            }

            fileBytesToSend_ = length;
        }
        LARGE_INTEGER li;
        li.QuadPart = offset;
        if (!SetFilePointerEx(sendHandle_, li, nullptr, FILE_BEGIN))
        {
            LOG_SYSERR << fileName << " seek error";
            CloseHandle(sendHandle_);
            sendHandle_ = INVALID_HANDLE_VALUE;
            isDone_ = true;
            return;
        }
        msgBufferPtr_ = std::make_unique<MsgBuffer>(
            kMaxSendFileBufferSize < fileBytesToSend_ ? kMaxSendFileBufferSize
                                                      : fileBytesToSend_);
    }

    bool isFile() const override
    {
        return true;
    }

    void getData(const char *&data, size_t &len) override
    {
        if (msgBufferPtr_->readableBytes() == 0 && fileBytesToSend_ > 0 &&
            sendHandle_ != INVALID_HANDLE_VALUE)
        {
            msgBufferPtr_->ensureWritableBytes(kMaxSendFileBufferSize <
                                                       fileBytesToSend_
                                                   ? kMaxSendFileBufferSize
                                                   : fileBytesToSend_);
            DWORD n = 0;
            if (!ReadFile(sendHandle_,
                          msgBufferPtr_->beginWrite(),
                          (uint32_t)msgBufferPtr_->writableBytes(),
                          &n,
                          nullptr))
            {
                LOG_SYSERR << "FileBufferNode::getData()";
            }
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
        msgBufferPtr_->retrieve(len);
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
        if (sendHandle_ != INVALID_HANDLE_VALUE)
        {
            CloseHandle(sendHandle_);
        }
    }
    int getFd() const override
    {
        LOG_ERROR << "getFd() is not supported on Windows";
        return 0;
    }
    bool available() const override
    {
        return sendHandle_ != INVALID_HANDLE_VALUE;
    }

  private:
    HANDLE sendHandle_{INVALID_HANDLE_VALUE};
    long long fileBytesToSend_{0};
    std::unique_ptr<MsgBuffer> msgBufferPtr_;
};
BufferNodePtr BufferNode::newFileBufferNode(const wchar_t *fileName,
                                            long long offset,
                                            long long length)
{
    return std::make_shared<FileBufferNode>(fileName, offset, length);
}
}  // namespace trantor