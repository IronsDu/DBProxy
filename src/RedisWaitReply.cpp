#include <assert.h>

#include "Client.h"
#include "RedisRequest.h"
#include "RedisParse.h"

#include "RedisWaitReply.h"

RedisSingleWaitReply::RedisSingleWaitReply(ClientLogicSession* client) : BaseWaitReply(client)
{
}

/*  TODO::如果这个回复就是第一个pending reply，那么可以不用缓存而直接发送给客户端(减少内存拷贝)  */
void RedisSingleWaitReply::onBackendReply(int64_t dbServerSocketID, BackendParseMsg& msg)
{
    assert(mWaitResponses.size() == 1);
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocketID == dbServerSocketID)
        {
            v.responseBinary = msg.transfer();
            break;
        }
    }
}

void RedisSingleWaitReply::mergeAndSend(ClientLogicSession* client)
{
    if (mErrorCode != nullptr)
    {
        RedisErrorReply tmp(client, mErrorCode->c_str());
        BaseWaitReply* f = &tmp;
        f->mergeAndSend(client);
    }
    else
    {
        client->cacheSend(mWaitResponses.front().responseBinary);
    }
}

RedisStatusReply::RedisStatusReply(ClientLogicSession* client, const char* status) : BaseWaitReply(client), mStatus(status)
{
}

void RedisStatusReply::onBackendReply(int64_t dbServerSocketID, BackendParseMsg&)
{
}

void RedisStatusReply::mergeAndSend(ClientLogicSession* client)
{
    std::string tmp = "+" + mStatus;
    tmp += "\r\n";
    client->cacheSend(tmp.c_str(), tmp.size());
}

RedisErrorReply::RedisErrorReply(ClientLogicSession* client, const char* error) : BaseWaitReply(client), mErrorCode(error)
{
}

void RedisErrorReply::onBackendReply(int64_t dbServerSocketID, BackendParseMsg&)
{
}

void RedisErrorReply::mergeAndSend(ClientLogicSession* client)
{
    std::string tmp = "-ERR " + mErrorCode;
    tmp += "\r\n";
    client->cacheSend(tmp.c_str(), tmp.size());
}

RedisWrongTypeReply::RedisWrongTypeReply(ClientLogicSession* client, const char* wrongType, const char* detail) :
    BaseWaitReply(client), mWrongType(wrongType), mWrongDetail(detail)
{
}

void RedisWrongTypeReply::onBackendReply(int64_t dbServerSocketID, BackendParseMsg&)
{
}

void RedisWrongTypeReply::mergeAndSend(ClientLogicSession* client)
{
    std::string tmp = "-WRONGTYPE " + mWrongType + " " + mWrongDetail;
    tmp += "\r\n";
    client->cacheSend(tmp.c_str(), tmp.size());
}

RedisMgetWaitReply::RedisMgetWaitReply(ClientLogicSession* client) : BaseWaitReply(client)
{
}

void RedisMgetWaitReply::onBackendReply(int64_t dbServerSocketID, BackendParseMsg& msg)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocketID == dbServerSocketID)
        {
            if (mWaitResponses.size() != 1)
            {
                v.redisReply = msg.redisReply;
                msg.redisReply = nullptr;
            }
            else
            {
                v.responseBinary = msg.transfer();
            }

            break;
        }
    }
}

void RedisMgetWaitReply::mergeAndSend(ClientLogicSession* client)
{
    if (mErrorCode != nullptr)
    {
        RedisErrorReply tmp(client, mErrorCode->c_str());
        BaseWaitReply* f = &tmp;
        f->mergeAndSend(client);
    }
    else
    {
        if (mWaitResponses.size() == 1)
        {
            client->cacheSend(mWaitResponses.front().responseBinary);
        }
        else
        {
            struct Bytes
            {
                const char* str;
                size_t len;
            };

            static vector<Bytes> vs;
            vs.clear();

            for (auto& v : mWaitResponses)
            {
                for (size_t i = 0; i < v.redisReply->reply->elements; ++i)
                {
                    vs.push_back({ v.redisReply->reply->element[i]->str, v.redisReply->reply->element[i]->len});
                }
            }

            static RedisProtocolRequest strsResponse;
            strsResponse.init();

            for (auto& v : vs)
            {
                strsResponse.appendBinary(v.str, v.len);
            }

            strsResponse.endl();
            client->cacheSend(strsResponse.getResult(), strsResponse.getResultLen());
        }
    }
}

RedisMsetWaitReply::RedisMsetWaitReply(ClientLogicSession* client) : BaseWaitReply(client)
{
}

void RedisMsetWaitReply::onBackendReply(int64_t dbServerSocketID, BackendParseMsg&)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocketID == dbServerSocketID)
        {
            /*  只需要强制设置成功，不需要保存任何reply数据     */
            v.forceOK = true;
            break;
        }
    }
}

void RedisMsetWaitReply::mergeAndSend(ClientLogicSession* client)
{
    if (mErrorCode != nullptr)
    {
        RedisErrorReply tmp(client, mErrorCode->c_str());
        BaseWaitReply* f = &tmp;
        f->mergeAndSend(client);
    }
    else
    {
        /*  mset总是成功,不需要合并后端服务器的reply   */
        const char* OK = "+OK\r\n";
        static int OK_LEN = strlen(OK);

        client->cacheSend(OK, OK_LEN);
    }
}

RedisDelWaitReply::RedisDelWaitReply(ClientLogicSession* client) : BaseWaitReply(client)
{
}

void RedisDelWaitReply::onBackendReply(int64_t dbServerSocketID, BackendParseMsg& msg)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocketID == dbServerSocketID)
        {
            if (mWaitResponses.size() != 1)
            {
                v.redisReply = msg.redisReply;
                msg.redisReply = nullptr;
            }
            else
            {
                v.responseBinary = msg.transfer();
            }

            break;
        }
    }
}

void RedisDelWaitReply::mergeAndSend(ClientLogicSession* client)
{
    if (mErrorCode != nullptr)
    {
        RedisErrorReply tmp(client, mErrorCode->c_str());
        BaseWaitReply* f = &tmp;
        f->mergeAndSend(client);
    }
    else
    {
        if (mWaitResponses.size() == 1)
        {
            /*TODO::诸如此类，直接将responseBinary作为socket的packet ptr，避免重复构造内存*/
            client->cacheSend(mWaitResponses.front().responseBinary);
        }
        else
        {
            int64_t num = 0;

            for (auto& v : mWaitResponses)
            {
                num += v.redisReply->reply->integer;
            }

            char tmp[1024];
            int len = sprintf(tmp, ":%lld\r\n", num);
            client->cacheSend(tmp, len);
        }
    }
}