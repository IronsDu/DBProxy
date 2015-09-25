#include <assert.h>
#include "Client.h"

#include "SSDBProtocol.h"
#include "SSDBWaitReply.h"

SSDBSingleWaitReply::SSDBSingleWaitReply(ClientLogicSession* client) : BaseWaitReply(client)
{
}

void SSDBSingleWaitReply::onBackendReply(int64_t dbServerSocketID, const char* buffer, int len)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocketID == dbServerSocketID)
        {
            v.reply = new std::string(buffer, len);
            break;
        }
    }
}

void SSDBSingleWaitReply::mergeAndSend(ClientLogicSession* client)
{
    assert(mWaitResponses.size() == 1);
    if (!mIsError)
    {
        client->send(mWaitResponses.front().reply->c_str(), mWaitResponses.front().reply->size());
    }
    else
    {
        /*  TODO::缓存错误的response对象,进行多次使用，避免多次构造   */
        SSDBProtocolRequest errorResponse;
        errorResponse.writev("error");
        errorResponse.endl();
        client->send(errorResponse.getResult(), errorResponse.getResultLen());
    }
}

SSDBMultiSetWaitReply::SSDBMultiSetWaitReply(ClientLogicSession* client) : BaseWaitReply(client)
{
}

void SSDBMultiSetWaitReply::onBackendReply(int64_t dbServerSocketID, const char* buffer, int len)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocketID == dbServerSocketID)
        {
            v.reply = new std::string(buffer, len);

            if (mWaitResponses.size() != 1)
            {
                v.ssdbReply = new SSDBProtocolResponse;
                v.ssdbReply->parse(v.reply->c_str());
            }

            break;
        }
    }
}

void SSDBMultiSetWaitReply::mergeAndSend(ClientLogicSession* client)
{
    if (mIsError)
    {
        SSDBProtocolRequest errorResponse;
        errorResponse.writev("error");
        errorResponse.endl();
        client->send(errorResponse.getResult(), errorResponse.getResultLen());
    }
    else
    {
        if (mWaitResponses.size() == 1)
        {
            client->send(mWaitResponses.front().reply->c_str(), mWaitResponses.front().reply->size());
        }
        else
        {
            string* errorReply = nullptr;
            int64_t num = 0;

            SSDBProtocolRequest megreResponse;
            for (auto& v : mWaitResponses)
            {
                int64_t tmp;
                if (read_int64(v.ssdbReply, &tmp).ok())
                {
                    num += tmp;
                }
                else
                {
                    errorReply = v.reply;
                    break;
                }
            }

            if (errorReply != nullptr)
            {
                client->send(errorReply->c_str(), errorReply->size());
            }
            else
            {
                SSDBProtocolRequest megreResponse;
                megreResponse.writev("ok", num);
                megreResponse.endl();
                client->send(megreResponse.getResult(), megreResponse.getResultLen());
            }
        }
    }
}

SSDBMultiGetWaitReply::SSDBMultiGetWaitReply(ClientLogicSession* client) : BaseWaitReply(client)
{
}

void SSDBMultiGetWaitReply::onBackendReply(int64_t dbServerSocketID, const char* buffer, int len)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocketID == dbServerSocketID)
        {
            v.reply = new std::string(buffer, len);

            if (mWaitResponses.size() != 1)
            {
                v.ssdbReply = new SSDBProtocolResponse;
                v.ssdbReply->parse(v.reply->c_str());
            }

            break;
        }
    }
}

void SSDBMultiGetWaitReply::mergeAndSend(ClientLogicSession* client)
{
    if (mIsError)
    {
        SSDBProtocolRequest errorResponse;
        errorResponse.writev("error");
        errorResponse.endl();
        client->send(errorResponse.getResult(), errorResponse.getResultLen());
    }
    else
    {
        if (mWaitResponses.size() == 1)
        {
            client->send(mWaitResponses.front().reply->c_str(), mWaitResponses.front().reply->size());
        }
        else
        {
            string* errorReply = nullptr;
            int64_t num = 0;

            SSDBProtocolRequest megreResponse;
            vector<string> kvs;

            for (auto& v : mWaitResponses)
            {
                if (!read_list(v.ssdbReply, &kvs).ok())
                {
                    errorReply = v.reply;
                    break;
                }
            }

            if (errorReply != nullptr)
            {
                client->send(errorReply->c_str(), errorReply->size());
            }
            else
            {
                SSDBProtocolRequest megreResponse;
                megreResponse.writev("ok", kvs);
                megreResponse.endl();
                client->send(megreResponse.getResult(), megreResponse.getResultLen());
            }
        }
    }
}