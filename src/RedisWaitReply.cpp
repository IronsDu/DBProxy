#include <assert.h>

#include "Client.h"
#include "RedisRequest.h"
#include "RedisWaitReply.h"

RedisSingleWaitReply::RedisSingleWaitReply(ClientLogicSession* client) : BaseWaitReply(client)
{
}

/*  TODO::如果这个回复就是第一个pending reply，那么可以不用缓存而直接发送给客户端(减少内存拷贝)  */
void RedisSingleWaitReply::onBackendReply(int64_t dbServerSocketID, const char* buffer, int len)
{
    assert(mWaitResponses.size() == 1);
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocketID == dbServerSocketID)
        {
            v.reply = new std::string(buffer, len);
            break;
        }
    }
}

void RedisSingleWaitReply::mergeAndSend(ClientLogicSession* client)
{
    if (!mIsError)
    {
        client->send(mWaitResponses.front().reply->c_str(), mWaitResponses.front().reply->size());
    }
    else
    {
        /*  TODO::缓存错误的response对象,进行多次使用，避免多次构造   */
        RedisErrorReply tmp(client, "error");   /*todo::使用错误码构造*/
        BaseWaitReply* f = &tmp;
        f->mergeAndSend(client);
    }
}

RedisStatusReply::RedisStatusReply(ClientLogicSession* client, const char* status) : BaseWaitReply(client), mStatus(status)
{
}

void RedisStatusReply::onBackendReply(int64_t dbServerSocketID, const char* buffer, int len)
{
}

void RedisStatusReply::mergeAndSend(ClientLogicSession* client)
{
    std::string tmp = "+" + mStatus;
    tmp += "\r\n";
    client->send(tmp.c_str(), tmp.size());
}

RedisErrorReply::RedisErrorReply(ClientLogicSession* client, const char* error) : BaseWaitReply(client), mError(error)
{
}

void RedisErrorReply::onBackendReply(int64_t dbServerSocketID, const char* buffer, int len)
{
}

void RedisErrorReply::mergeAndSend(ClientLogicSession* client)
{
    std::string tmp = "-ERR " + mError;
    tmp += "\r\n";
    client->send(tmp.c_str(), tmp.size());
}

RedisWrongTypeReply::RedisWrongTypeReply(ClientLogicSession* client, const char* wrongType, const char* detail) :
    BaseWaitReply(client), mWrongType(wrongType), mWrongDetail(detail)
{
}

void RedisWrongTypeReply::onBackendReply(int64_t dbServerSocketID, const char* buffer, int len)
{
}

void RedisWrongTypeReply::mergeAndSend(ClientLogicSession* client)
{
    std::string tmp = "-WRONGTYPE " + mWrongType + " " + mWrongDetail;
    tmp += "\r\n";
    client->send(tmp.c_str(), tmp.size());
}