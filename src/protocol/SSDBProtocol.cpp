#include "SSDBProtocol.h"

#include <stdlib.h>
#include <string.h>

#include <brynet/base/Buffer.hpp>
#include <brynet/base/Platform.hpp>
#include <brynet/net/SocketLibFunction.hpp>

#if defined PLATFORM_WINDOWS
#define snprintf _snprintf
#endif

using namespace brynet::base;

Status::Status()
    : mCacheStatus(STATUS_TYPE::STATUS_NONE)
{
}

Status::Status(const std::string& code)
    : mCode(code),
      mCacheStatus(STATUS_TYPE::STATUS_NONE)
{
    cacheCodeType();
}

Status::Status(std::string&& code)
    : mCode(std::move(code)),
      mCacheStatus(STATUS_TYPE::STATUS_NONE)
{
    cacheCodeType();
}

Status::Status(Status&& s) noexcept
    : mCode(std::move(s.mCode)),
      mCacheStatus(s.mCacheStatus)
{
}

Status& Status::operator=(Status&& s) noexcept
{
    if (this != &s)
    {
        mCode = std::move(s.mCode);
        mCacheStatus = s.mCacheStatus;
    }

    return *this;
}

void Status::cacheCodeType()
{
    if (mCacheStatus == STATUS_TYPE::STATUS_NONE)
    {
        if (mCode == "ok")
        {
            mCacheStatus = STATUS_TYPE::STATUS_OK;
        }
        else if (mCode == "not_found")
        {
            mCacheStatus = STATUS_TYPE::STATUS_NOTFOUND;
        }
        else
        {
            mCacheStatus = STATUS_TYPE::STATUS_ERROR;
        }
    }
}

bool Status::not_found() const
{
    return mCacheStatus == STATUS_TYPE::STATUS_NOTFOUND;
}

bool Status::ok() const
{
    return mCacheStatus == STATUS_TYPE::STATUS_OK;
}

bool Status::error() const
{
    return mCacheStatus == STATUS_TYPE::STATUS_ERROR;
}

const std::string& Status::code() const
{
    return mCode;
}

SSDBProtocolRequest::SSDBProtocolRequest()
{
    m_request = brynet::base::buffer_new(DEFAULT_SSDBPROTOCOL_LEN);
}

SSDBProtocolRequest::~SSDBProtocolRequest()
{
    buffer_delete(m_request);
    m_request = nullptr;
}

void SSDBProtocolRequest::appendStr(const char* str)
{
    size_t len = strlen(str);
    char lenstr[16];
    int num = snprintf(lenstr, sizeof(len), "%lld\n", len);
    appendBlock(lenstr, num);
    appendBlock(str, len);
    appendBlock("\n", 1);
}

void SSDBProtocolRequest::appendStr(const char* str, size_t len)
{
    char lenstr[16];
    int num = snprintf(lenstr, sizeof(len), "%lld\n", len);
    appendBlock(lenstr, num);
    appendBlock(str, len);
    appendBlock("\n", 1);
}

void SSDBProtocolRequest::appendInt64(int64_t val)
{
    char str[30];
    snprintf(str, sizeof(str), "%lld", val);
    appendStr(str);
}

void SSDBProtocolRequest::appendStr(const std::string& str)
{
    char len[16];
    int num = snprintf(len, sizeof(len), "%lld\n", str.size());
    appendBlock(len, num);
    appendBlock(str.c_str(), str.length());
    appendBlock("\n", 1);
}

void SSDBProtocolRequest::endl()
{
    appendBlock("\n", 1);
}

void SSDBProtocolRequest::appendBlock(const char* data, size_t len)
{
    if (buffer_getwritevalidcount(m_request) < len)
    {
        brynet::base::buffer_s* temp = buffer_new(buffer_getsize(m_request) + len);
        memcpy(buffer_getwriteptr(temp), buffer_getreadptr(m_request), buffer_getreadvalidcount(m_request));
        buffer_addwritepos(temp, buffer_getreadvalidcount(m_request));
        buffer_delete(m_request);
        m_request = temp;
    }

    buffer_write(m_request, data, len);
}

const char* SSDBProtocolRequest::getResult()
{
    return buffer_getreadptr(m_request);
}

size_t SSDBProtocolRequest::getResultLen()
{
    return buffer_getreadvalidcount(m_request);
}

void SSDBProtocolRequest::init()
{
    buffer_init(m_request);
}

SSDBProtocolResponse::~SSDBProtocolResponse()
{
}

void SSDBProtocolResponse::init()
{
    mBuffers.clear();
}

void SSDBProtocolResponse::parse(const char* buffer)
{
    const char* current = buffer;
    while (true)
    {
        char* temp;
        int datasize = strtol(current, &temp, 10);
        current = temp;
        current += 1;
        Bytes tmp = {current, datasize};
        mBuffers.push_back(tmp);
        current += datasize;

        current += 1;

        if (*current == '\n')
        {
            current += 1;
            break;
        }
    }
}

Bytes* SSDBProtocolResponse::getByIndex(size_t index)
{
    if (mBuffers.size() > index)
    {
        return &mBuffers[index];
    }
    else
    {
        const static char* nullstr = "null";
        static Bytes nullbuffer = {nullstr, strlen(nullstr) + 1};
        return &nullbuffer;
    }
}

void SSDBProtocolResponse::pushByte(const char* buffer, size_t len)
{
    Bytes tmp = {buffer, len};
    mBuffers.push_back(tmp);
}

size_t SSDBProtocolResponse::getBuffersLen() const
{
    return mBuffers.size();
}

Status SSDBProtocolResponse::getStatus()
{
    if (mBuffers.empty())
    {
        return Status("error");
    }

    return std::string(mBuffers[0].buffer, mBuffers[0].len);
}

int SSDBProtocolResponse::check_ssdb_packet(const char* buffer, size_t len)
{
    const char* end = buffer + len;
    const char* current = buffer;

    while (true)
    {
        char* temp;
        int datasize = strtol(current, &temp, 10);
        if (datasize == 0 && temp == current)
        {
            break;
        }
        current = temp;

        if (current >= end || *current != '\n')
        {
            break;
        }
        current += 1;
        current += datasize;

        if (current >= end || *current != '\n')
        {
            break;
        }

        current += 1;

        if (current >= end)
        {
            break;
        }
        else if (*current == '\n')
        {
            current += 1;
            return (current - buffer);
        }
    }

    return 0;
}

Status read_bytes(SSDBProtocolResponse* response, std::vector<Bytes>& ret)
{
    Status status = response->getStatus();
    if (status.ok())
    {
        for (size_t i = 1; i < response->getBuffersLen(); ++i)
        {
            ret.push_back(*(response->getByIndex(i)));
        }
    }

    return status;
}

Status read_list(SSDBProtocolResponse* response, std::vector<std::string>& ret)
{
    Status status = response->getStatus();
    if (status.ok())
    {
        for (size_t i = 1; i < response->getBuffersLen(); ++i)
        {
            Bytes* buffer = response->getByIndex(i);
            ret.emplace_back(std::string(buffer->buffer, buffer->len));
        }
    }

    return status;
}

Status read_int64(SSDBProtocolResponse* response, int64_t& ret)
{
    Status status = response->getStatus();
    if (status.ok())
    {
        if (response->getBuffersLen() >= 2)
        {
            Bytes* buf = response->getByIndex(1);
            std::string temp(buf->buffer, buf->len);
            (void) sscanf(temp.c_str(), "%lld", &ret);
        }
        else
        {
            status = Status("server_error");
        }
    }

    return status;
}

Status read_byte(SSDBProtocolResponse* response, Bytes& ret)
{
    Status status = response->getStatus();
    if (status.ok())
    {
        if (response->getBuffersLen() >= 2)
        {
            ret = *(response->getByIndex(1));
        }
        else
        {
            status = Status("server_error");
        }
    }

    return status;
}

Status read_str(SSDBProtocolResponse* response, std::string& ret)
{
    Status status = response->getStatus();
    if (status.ok())
    {
        if (response->getBuffersLen() >= 2)
        {
            Bytes* buf = response->getByIndex(1);
            ret = std::string(buf->buffer, buf->len);
        }
        else
        {
            status = Status("server_error");
        }
    }

    return status;
}
