#ifndef _REDIS_PROTOCOL_H
#define _REDIS_PROTOCOL_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <unordered_map>
#include <vector>

class RedisProtocolRequest
{
public:
    RedisProtocolRequest() = default;

    void init()
    {
        mArgc = 0;
        mResult.clear();
    }

    template<typename Arg1, typename... Args>
    void writev(const Arg1& arg1, const Args&... args)
    {
        this->operator<<(arg1);
        writev(args...);
    }

    void endl()
    {
        std::string tmp;
        tmp.push_back('*');
        tmp += std::to_string(mArgc);
        tmp += "\r\n";
        tmp += mResult;
        mResult = std::move(tmp);
    }

    void appendBinary(const char* buffer, size_t len)
    {
        addStr(buffer, len);
    }

    void writev()
    {
    }

    const char* getResult() const
    {
        return mResult.c_str();
    }

    int getResultLen() const
    {
        return mResult.size();
    }

private:
    RedisProtocolRequest& operator<<(const std::vector<std::string>& keys)
    {
        for (const auto& v : keys)
        {
            addStr(v);
        }
        return *this;
    }

    RedisProtocolRequest& operator<<(const std::unordered_map<std::string, std::string>& kvs)
    {
        for (const auto& [k, v] : kvs)
        {
            addStr(k);
            addStr(v);
        }
        return *this;
    }

    RedisProtocolRequest& operator<<(const int64_t& v)
    {
        addStr(std::to_string(v));
        return *this;
    }
    RedisProtocolRequest& operator<<(const char* const& v)
    {
        addStr(v, strlen(v));
        return *this;
    }
    RedisProtocolRequest& operator<<(const std::string& v)
    {
        addStr(v);
        return *this;
    }

private:
    void addStr(const std::string& arg)
    {
        addStr(arg.c_str(), arg.size());
    }

    void addStr(const char* buffer, size_t len)
    {
        mResult.push_back('$');
        mResult += (std::to_string(len));
        mResult += "\r\n";
        mResult.append(buffer, len);
        mResult += "\r\n";
        mArgc += 1;
    }

private:
    int mArgc = {0};
    std::string mResult;
};

#endif
