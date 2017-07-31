#include <assert.h>
#include "Client.h"

#include "protocol/SSDBProtocol.h"
#include "SSDBWaitReply.h"

static const std::string SSDB_OK = "ok";
static const std::string SSDB_ERROR = "error";

static void syncSSDBStrList(const ClientSession::PTR& client, const std::vector<std::string>& strList)
{
    SSDBProtocolRequest& strsResponse = client->getCacheSSDBProtocol();
    strsResponse.init();

    strsResponse.writev(strList);
    strsResponse.endl();

    client->sendPacket(strsResponse.getResult(), strsResponse.getResultLen());
}

StrListSSDBReply::StrListSSDBReply(const ClientSession::PTR& client) : BaseWaitReply(client)
{
}

void StrListSSDBReply::onBackendReply(int64_t dbServerSocketID, const BackendParseMsg::PTR&)
{
}

void StrListSSDBReply::mergeAndSend(const ClientSession::PTR& client)
{
    mStrListResponse.endl();
    client->sendPacket(mStrListResponse.getResult(), mStrListResponse.getResultLen());
}

void StrListSSDBReply::pushStr(std::string&& str)
{
    mStrListResponse.writev(str);
}

void StrListSSDBReply::pushStr(const std::string& str)
{
    mStrListResponse.writev(str);
}

void StrListSSDBReply::pushStr(const char* str)
{
    mStrListResponse.appendStr(str);
}

SSDBSingleWaitReply::SSDBSingleWaitReply(const ClientSession::PTR& client) : BaseWaitReply(client)
{
}

void SSDBSingleWaitReply::onBackendReply(int64_t dbServerSocketID, const BackendParseMsg::PTR& msg)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocketID == dbServerSocketID)
        {
            v.responseBinary = std::move(msg->responseMemory);
            break;
        }
    }
}

void SSDBSingleWaitReply::mergeAndSend(const ClientSession::PTR& client)
{
    if (!mErrorCode.empty())
    {
        syncSSDBStrList(client, { SSDB_ERROR, mErrorCode });
    }
    else
    {
        client->sendPacket(std::move(mWaitResponses.front().responseBinary));
    }
}

SSDBMultiSetWaitReply::SSDBMultiSetWaitReply(const ClientSession::PTR& client) : BaseWaitReply(client)
{
}

void SSDBMultiSetWaitReply::onBackendReply(int64_t dbServerSocketID, const BackendParseMsg::PTR& msg)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocketID != dbServerSocketID)
        {
            continue;
        }

        v.responseBinary = std::move(msg->responseMemory);

        if (mWaitResponses.size() != 1)
        {
            v.ssdbReply = std::make_shared<SSDBProtocolResponse>();
            v.ssdbReply->parse(v.responseBinary->c_str());
        }
        break;
    }
}

void SSDBMultiSetWaitReply::mergeAndSend(const ClientSession::PTR& client)
{
    if (!mErrorCode.empty())
    {
        syncSSDBStrList(client, { SSDB_ERROR, mErrorCode });
    }
    else
    {
        if (mWaitResponses.size() == 1)
        {
            client->sendPacket(std::move(mWaitResponses.front().responseBinary));
        }
        else
        {
            std::shared_ptr<std::string> errorReply = nullptr;
            int64_t num = 0;

            for (auto& v : mWaitResponses)
            {
                int64_t tmp;
                if (read_int64(v.ssdbReply.get(), &tmp).ok())
                {
                    num += tmp;
                }
                else
                {
                    errorReply = std::move(v.responseBinary);
                    break;
                }
            }

            if (errorReply != nullptr)
            {
                client->sendPacket(std::move(errorReply));
            }
            else
            {
                SSDBProtocolRequest& response = client->getCacheSSDBProtocol();
                response.init();

                response.writev(SSDB_OK, num);
                response.endl();
                client->sendPacket(response.getResult(), response.getResultLen());
            }
        }
    }
}

SSDBMultiGetWaitReply::SSDBMultiGetWaitReply(const ClientSession::PTR& client) : BaseWaitReply(client)
{
}

void SSDBMultiGetWaitReply::onBackendReply(int64_t dbServerSocketID, const BackendParseMsg::PTR& msg)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocketID != dbServerSocketID)
        {
            continue;
        }

        v.responseBinary = std::move(msg->responseMemory);

        if (mWaitResponses.size() != 1)
        {
            v.ssdbReply = std::make_shared<SSDBProtocolResponse>();
            v.ssdbReply->parse(v.responseBinary->c_str());
        }
        break;
    }
}

void SSDBMultiGetWaitReply::mergeAndSend(const ClientSession::PTR& client)
{
    if (!mErrorCode.empty())
    {
        syncSSDBStrList(client, { SSDB_ERROR, mErrorCode });
    }
    else
    {
        if (mWaitResponses.size() == 1)
        {
            client->sendPacket(std::move(mWaitResponses.front().responseBinary));
        }
        else
        {
            std::shared_ptr<std::string> errorReply = nullptr;

            std::vector<Bytes> kvs;

            for (auto& v : mWaitResponses)
            {
                if (!read_bytes(v.ssdbReply.get(), &kvs).ok())
                {
                    errorReply = std::move(v.responseBinary);
                    break;
                }
            }

            if (errorReply != nullptr)
            {
                client->sendPacket(std::move(errorReply));
            }
            else
            {
                SSDBProtocolRequest& strsResponse = client->getCacheSSDBProtocol();
                strsResponse.init();

                strsResponse.writev(SSDB_OK);
                for (auto& v : kvs)
                {
                    strsResponse.appendStr(v.buffer, v.len);
                }
                
                strsResponse.endl();
                client->sendPacket(strsResponse.getResult(), strsResponse.getResultLen());
            }
        }
    }
}