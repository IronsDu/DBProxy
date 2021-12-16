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

    client->send(strsResponse.getResult(), strsResponse.getResultLen());
}

StrListSSDBReply::StrListSSDBReply(const ClientSession::PTR& client) : BaseWaitReply(client)
{
}

void StrListSSDBReply::onBackendReply(brynet::net::TcpConnection::Ptr dbServerSocket, const BackendParseMsg::PTR&)
{
}

void StrListSSDBReply::mergeAndSend(const ClientSession::PTR& client)
{
    mStrListResponse.endl();
    client->send(mStrListResponse.getResult(), mStrListResponse.getResultLen());
}

void StrListSSDBReply::pushStr(std::string&& str)
{
    mStrListResponse.writev(std::move(str));
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

void SSDBSingleWaitReply::onBackendReply(brynet::net::TcpConnection::Ptr dbServerSocket, const BackendParseMsg::PTR& msg)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocket == dbServerSocket)
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
        client->send(mWaitResponses.front().responseBinary);
    }
}

SSDBMultiSetWaitReply::SSDBMultiSetWaitReply(const ClientSession::PTR& client) : BaseWaitReply(client)
{
}

void SSDBMultiSetWaitReply::onBackendReply(brynet::net::TcpConnection::Ptr dbServerSocket, const BackendParseMsg::PTR& msg)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocket != dbServerSocket)
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
        return;
    }
    if (mWaitResponses.size() == 1)
    {
        client->send(mWaitResponses.front().responseBinary);
        return;
    }

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
        client->send(errorReply);
    }
    else
    {
        SSDBProtocolRequest& response = client->getCacheSSDBProtocol();
        response.init();

        response.writev(SSDB_OK, num);
        response.endl();
        client->send(response.getResult(), response.getResultLen());
    }
}

SSDBMultiGetWaitReply::SSDBMultiGetWaitReply(const ClientSession::PTR& client) : BaseWaitReply(client)
{
}

void SSDBMultiGetWaitReply::onBackendReply(brynet::net::TcpConnection::Ptr dbServerSocket, const BackendParseMsg::PTR& msg)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocket != dbServerSocket)
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
        return;
    }
    if (mWaitResponses.size() == 1)
    {
        client->send(mWaitResponses.front().responseBinary);
        return;
    }

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
        client->send(std::move(errorReply));
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
        client->send(strsResponse.getResult(), strsResponse.getResultLen());
    }
}