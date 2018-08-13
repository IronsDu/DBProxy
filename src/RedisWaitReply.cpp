#include <assert.h>

#include "Client.h"
#include "protocol/RedisRequest.h"
#include "protocol/RedisParse.h"

#include "RedisWaitReply.h"

using namespace std;

static void HelpSendError(const shared_ptr<ClientSession>& client, const string& error)
{
    BaseWaitReply::PTR tmp = std::make_shared<RedisErrorReply>(client, error.c_str());
    tmp->mergeAndSend(client);
}

RedisSingleWaitReply::RedisSingleWaitReply(const ClientSession::PTR& client) : BaseWaitReply(client)
{
}

void RedisSingleWaitReply::onBackendReply(brynet::net::DataSocket::PTR dbServerSocket, const BackendParseMsg::PTR& msg)
{
    assert(mWaitResponses.size() == 1);
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocket == dbServerSocket)
        {
            v.responseBinary = std::move(msg->responseMemory);
            break;
        }
    }
}

void RedisSingleWaitReply::mergeAndSend(const ClientSession::PTR& client)
{
    if (!mErrorCode.empty())
    {
        HelpSendError(client, mErrorCode);
    }
    else if (!mWaitResponses.empty())
    {
        client->send(mWaitResponses.front().responseBinary);
    }
}

RedisStatusReply::RedisStatusReply(const ClientSession::PTR& client, const char* status) : BaseWaitReply(client), mStatus(status)
{
}

void RedisStatusReply::onBackendReply(brynet::net::DataSocket::PTR dbServerSocket, const BackendParseMsg::PTR&)
{
}

void RedisStatusReply::mergeAndSend(const ClientSession::PTR& client)
{
    std::shared_ptr<std::string> tmp = std::make_shared<string>("+");
    tmp->append(mStatus);
    tmp->append("\r\n");
    client->send(tmp);
}

RedisErrorReply::RedisErrorReply(const ClientSession::PTR& client, const char* error) : BaseWaitReply(client), mErrorCode(error)
{
}

void RedisErrorReply::onBackendReply(brynet::net::DataSocket::PTR dbServerSocket, const BackendParseMsg::PTR&)
{
}

void RedisErrorReply::mergeAndSend(const ClientSession::PTR& client)
{
    std::shared_ptr<std::string> tmp = std::make_shared<string>("-ERR ");
    tmp->append(mErrorCode);
    tmp->append("\r\n");
    client->send(tmp);
}

RedisWrongTypeReply::RedisWrongTypeReply(const ClientSession::PTR& client, const char* wrongType, const char* detail) :
    BaseWaitReply(client), mWrongType(wrongType), mWrongDetail(detail)
{
}

void RedisWrongTypeReply::onBackendReply(brynet::net::DataSocket::PTR dbServerSocket, const BackendParseMsg::PTR&)
{
}

void RedisWrongTypeReply::mergeAndSend(const ClientSession::PTR& client)
{
    std::shared_ptr<std::string> tmp = std::make_shared<string>("-WRONGTYPE ");
    tmp->append(mWrongType);
    tmp->append(" ");
    tmp->append(mWrongDetail);
    tmp->append("\r\n");
    client->send(tmp);
}

RedisMgetWaitReply::RedisMgetWaitReply(const ClientSession::PTR& client) : BaseWaitReply(client)
{
}

void RedisMgetWaitReply::onBackendReply(brynet::net::DataSocket::PTR dbServerSocket, const BackendParseMsg::PTR& msg)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocket != dbServerSocket)
        {
            continue;
        }

        if (mWaitResponses.size() != 1)
        {
            v.redisReply = std::move(msg->redisReply);
            msg->redisReply = nullptr;
        }
        else
        {
            v.responseBinary = std::move(msg->responseMemory);
        }
        break;
    }
}

void RedisMgetWaitReply::mergeAndSend(const ClientSession::PTR& client)
{
    if (!mErrorCode.empty())
    {
        HelpSendError(client, mErrorCode);
        return;
    }
    if (mWaitResponses.size() == 1)
    {
        client->send(mWaitResponses.front().responseBinary);
        return;
    }

    RedisProtocolRequest& strsResponse = client->getCacheRedisProtocol();
    strsResponse.init();

    for (const auto& v : mWaitResponses)
    {
        for (size_t i = 0; i < v.redisReply->reply->elements; ++i)
        {
            strsResponse.appendBinary(v.redisReply->reply->element[i]->str, v.redisReply->reply->element[i]->len);
        }
    }

    strsResponse.endl();
    client->send(strsResponse.getResult(), strsResponse.getResultLen());
}

RedisMsetWaitReply::RedisMsetWaitReply(const ClientSession::PTR& client) : BaseWaitReply(client)
{
}

void RedisMsetWaitReply::onBackendReply(brynet::net::DataSocket::PTR dbServerSocket, const BackendParseMsg::PTR&)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocket != dbServerSocket)
        {
            continue;
        }

        /*  只需要强制设置成功，不需要保存任何reply数据     */
        v.forceOK = true;
        break;
    }
}

void RedisMsetWaitReply::mergeAndSend(const ClientSession::PTR& client)
{
    if (!mErrorCode.empty())
    {
        HelpSendError(client, mErrorCode);
        return;
    }

    /*  mset总是成功,不需要合并后端服务器的reply   */
    const char* OK = "+OK\r\n";
    static int OK_LEN = strlen(OK);

    client->send(OK, OK_LEN);
}

RedisDelWaitReply::RedisDelWaitReply(const ClientSession::PTR& client) : BaseWaitReply(client)
{
}

void RedisDelWaitReply::onBackendReply(brynet::net::DataSocket::PTR dbServerSocket, const BackendParseMsg::PTR& msg)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocket != dbServerSocket)
        {
            continue;
        }

        if (mWaitResponses.size() != 1)
        {
            v.redisReply = std::move(msg->redisReply);
            msg->redisReply = nullptr;
        }
        else
        {
            v.responseBinary = std::move(msg->responseMemory);
        }
        break;
    }
}

void RedisDelWaitReply::mergeAndSend(const ClientSession::PTR& client)
{
    if (!mErrorCode.empty())
    {
        HelpSendError(client, mErrorCode);
        return;
    }
    if (mWaitResponses.size() == 1)
    {
        client->send(mWaitResponses.front().responseBinary);
        return;
    }

    int64_t num = 0;
    for (const auto& v : mWaitResponses)
    {
        num += v.redisReply->reply->integer;
    }

    char tmp[1024];
    int len = sprintf(tmp, ":%lld\r\n", num);
    client->send(tmp, len);
}