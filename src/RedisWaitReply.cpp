#include <assert.h>

#include "Client.h"
#include "protocol/RedisRequest.h"
#include "protocol/RedisParse.h"

#include "RedisWaitReply.h"

using namespace std;

static void HelpSendError(shared_ptr<ClientSession>& client, const string& error)
{
    RedisErrorReply tmp(client, error.c_str());
    ((BaseWaitReply*)&tmp)->mergeAndSend(client);
}

RedisSingleWaitReply::RedisSingleWaitReply(std::shared_ptr<ClientSession> client) : BaseWaitReply(client)
{
}

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

void RedisSingleWaitReply::mergeAndSend(std::shared_ptr<ClientSession>& client)
{
    if (mErrorCode != nullptr)
    {
        HelpSendError(client, *mErrorCode);
    }
    else if (!mWaitResponses.empty())
    {
        client->sendPacket(mWaitResponses.front().responseBinary);
    }
}

RedisStatusReply::RedisStatusReply(std::shared_ptr<ClientSession> client, const char* status) : BaseWaitReply(client), mStatus(status)
{
}

void RedisStatusReply::onBackendReply(int64_t dbServerSocketID, BackendParseMsg&)
{
}

void RedisStatusReply::mergeAndSend(std::shared_ptr<ClientSession>& client)
{
    std::shared_ptr<std::string> tmp = std::make_shared<string>("+");
    tmp->append(mStatus);
    tmp->append("\r\n");
    client->sendPacket(tmp);
}

RedisErrorReply::RedisErrorReply(std::shared_ptr<ClientSession> client, const char* error) : BaseWaitReply(client), mErrorCode(error)
{
}

void RedisErrorReply::onBackendReply(int64_t dbServerSocketID, BackendParseMsg&)
{
}

void RedisErrorReply::mergeAndSend(std::shared_ptr<ClientSession>& client)
{
    std::shared_ptr<std::string> tmp = std::make_shared<string>("-ERR ");
    tmp->append(mErrorCode);
    tmp->append("\r\n");
    client->sendPacket(tmp);
}

RedisWrongTypeReply::RedisWrongTypeReply(std::shared_ptr<ClientSession> client, const char* wrongType, const char* detail) :
    BaseWaitReply(client), mWrongType(wrongType), mWrongDetail(detail)
{
}

void RedisWrongTypeReply::onBackendReply(int64_t dbServerSocketID, BackendParseMsg&)
{
}

void RedisWrongTypeReply::mergeAndSend(std::shared_ptr<ClientSession>& client)
{
    std::shared_ptr<std::string> tmp = std::make_shared<string>("-WRONGTYPE ");
    tmp->append(mWrongType);
    tmp->append(" ");
    tmp->append(mWrongDetail);
    tmp->append("\r\n");
    client->sendPacket(tmp);
}

RedisMgetWaitReply::RedisMgetWaitReply(std::shared_ptr<ClientSession> client) : BaseWaitReply(client)
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

void RedisMgetWaitReply::mergeAndSend(std::shared_ptr<ClientSession>& client)
{
    if (mErrorCode != nullptr)
    {
        HelpSendError(client, *mErrorCode);
    }
    else
    {
        if (mWaitResponses.size() == 1)
        {
            client->sendPacket(mWaitResponses.front().responseBinary);
        }
        else
        {
            RedisProtocolRequest& strsResponse = client->getCacheRedisProtocol();
            strsResponse.init();

            for (auto& v : mWaitResponses)
            {
                for (size_t i = 0; i < v.redisReply->reply->elements; ++i)
                {
                    strsResponse.appendBinary(v.redisReply->reply->element[i]->str, v.redisReply->reply->element[i]->len);
                }
            }

            strsResponse.endl();
            client->sendPacket(strsResponse.getResult(), strsResponse.getResultLen());
        }
    }
}

RedisMsetWaitReply::RedisMsetWaitReply(std::shared_ptr<ClientSession> client) : BaseWaitReply(client)
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

void RedisMsetWaitReply::mergeAndSend(std::shared_ptr<ClientSession>& client)
{
    if (mErrorCode != nullptr)
    {
        HelpSendError(client, *mErrorCode);
    }
    else
    {
        /*  mset总是成功,不需要合并后端服务器的reply   */
        const char* OK = "+OK\r\n";
        static int OK_LEN = strlen(OK);

        client->sendPacket(OK, OK_LEN);
    }
}

RedisDelWaitReply::RedisDelWaitReply(std::shared_ptr<ClientSession> client) : BaseWaitReply(client)
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

void RedisDelWaitReply::mergeAndSend(std::shared_ptr<ClientSession>& client)
{
    if (mErrorCode != nullptr)
    {
        HelpSendError(client, *mErrorCode);
    }
    else
    {
        if (mWaitResponses.size() == 1)
        {
            client->sendPacket(mWaitResponses.front().responseBinary);
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
            client->sendPacket(tmp, len);
        }
    }
}