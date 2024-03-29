#ifndef _SSDB_PROTOCOL_H
#define _SSDB_PROTOCOL_H

#include <stdint.h>

#include <brynet/base/Buffer.hpp>
#include <brynet/base/NonCopyable.hpp>
#include <string>
#include <unordered_map>
#include <vector>

constexpr int DEFAULT_SSDBPROTOCOL_LEN = 1024;

struct buffer_s;

class Status
{
    enum class STATUS_TYPE
    {
        STATUS_NONE,
        STATUS_OK,
        STATUS_NOTFOUND,
        STATUS_ERROR,
    };

public:
    Status();
    Status(std::string&&);
    Status(const std::string& code);
    Status(Status&&) noexcept;

    Status& operator=(Status&&) noexcept;

    bool not_found() const;
    bool ok() const;
    bool error() const;

    const std::string& code() const;

private:
    void cacheCodeType();

private:
    std::string mCode;
    STATUS_TYPE mCacheStatus;
};

class SSDBProtocolRequest : private brynet::base::NonCopyable
{
public:
    SSDBProtocolRequest();

    virtual ~SSDBProtocolRequest();

    void appendStr(const char* str);
    void appendStr(const char* str, size_t len);
    void appendInt64(int64_t val);
    void appendStr(const std::string& str);

    void endl();

    const char* getResult();
    size_t getResultLen();

    void init();

    template<typename Arg1, typename... Args>
    void writev(const Arg1& arg1, const Args&... args)
    {
        this->operator<<(arg1);
        writev(args...);
    }

    void writev()
    {
    }

private:
    void appendBlock(const char* data, size_t len);

    SSDBProtocolRequest& operator<<(const std::vector<std::string>& keys)
    {
        for (auto& v : keys)
        {
            appendStr(v);
        }
        return *this;
    }

    SSDBProtocolRequest& operator<<(const std::unordered_map<std::string, std::string>& kvs)
    {
        for (const auto& [k, v] : kvs)
        {
            appendStr(k);
            appendStr(v);
        }
        return *this;
    }

    SSDBProtocolRequest& operator<<(int64_t v)
    {
        appendInt64(v);
        return *this;
    }

    SSDBProtocolRequest& operator<<(const char* const v)
    {
        appendStr(v);
        return *this;
    }

    SSDBProtocolRequest& operator<<(const std::string& v)
    {
        appendStr(v);
        return *this;
    }

private:
    struct brynet::base::buffer_s* m_request = nullptr;
};

struct Bytes {
    const char* buffer = nullptr;
    int len = 0;
};

class SSDBProtocolResponse
{
public:
    ~SSDBProtocolResponse();

    void init();

    void parse(const char* buffer);
    Bytes* getByIndex(size_t index);
    void pushByte(const char* buffer, size_t len);

    size_t getBuffersLen() const;

    Status getStatus();

    static int check_ssdb_packet(const char* buffer, size_t len);

private:
    std::vector<Bytes> mBuffers;
};

Status read_bytes(SSDBProtocolResponse* response, std::vector<Bytes>& ret);
Status read_list(SSDBProtocolResponse* response, std::vector<std::string>& ret);
Status read_int64(SSDBProtocolResponse* response, int64_t& ret);
Status read_byte(SSDBProtocolResponse* response, Bytes& ret);
Status read_str(SSDBProtocolResponse* response, std::string& ret);


#endif
