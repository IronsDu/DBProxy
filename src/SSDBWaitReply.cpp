#include <assert.h>
#include "Client.h"

#include "SSDBProtocol.h"
#include "SSDBWaitReply.h"

static const std::string SSDB_OK = "ok";
static const std::string SSDB_ERROR = "error";

static void syncSSDBStrList(ClientLogicSession* client, const std::vector<std::string>& strList)
{
    static SSDBProtocolRequest strsResponse;
    strsResponse.init();

    strsResponse.writev(strList);
    strsResponse.endl();

    client->cacheSend(strsResponse.getResult(), strsResponse.getResultLen());
}

StrListSSDBReply::StrListSSDBReply(ClientLogicSession* client) : BaseWaitReply(client)
{
}

void StrListSSDBReply::onBackendReply(int64_t dbServerSocketID, BackendParseMsg&)
{

}

void StrListSSDBReply::mergeAndSend(ClientLogicSession* client)
{
    mStrListResponse.endl();
    client->cacheSend(mStrListResponse.getResult(), mStrListResponse.getResultLen());
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

SSDBSingleWaitReply::SSDBSingleWaitReply(ClientLogicSession* client) : BaseWaitReply(client)
{
}

/*  TODO::如果这个回复就是第一个pending reply，那么可以不用缓存而直接发送给客户端(减少内存拷贝)  */
void SSDBSingleWaitReply::onBackendReply(int64_t dbServerSocketID, BackendParseMsg& msg)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocketID == dbServerSocketID)
        {
            v.responseBinary = msg.transfer();
            break;
        }
    }
}

void SSDBSingleWaitReply::mergeAndSend(ClientLogicSession* client)
{
    if (mErrorCode != nullptr)
    {
        syncSSDBStrList(client, { SSDB_ERROR, *mErrorCode });
    }
    else
    {
        client->cacheSend(mWaitResponses.front().responseBinary);
    }
}

SSDBMultiSetWaitReply::SSDBMultiSetWaitReply(ClientLogicSession* client) : BaseWaitReply(client)
{
}

void SSDBMultiSetWaitReply::onBackendReply(int64_t dbServerSocketID, BackendParseMsg& msg)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocketID == dbServerSocketID)
        {
            v.responseBinary = msg.transfer();

            if (mWaitResponses.size() != 1)
            {
                v.ssdbReply = new SSDBProtocolResponse;
                v.ssdbReply->parse(v.responseBinary->c_str());
            }

            break;
        }
    }
}

void SSDBMultiSetWaitReply::mergeAndSend(ClientLogicSession* client)
{
    if (mErrorCode != nullptr)
    {
        syncSSDBStrList(client, { SSDB_ERROR, *mErrorCode });
    }
    else
    {
        if (mWaitResponses.size() == 1)
        {
            client->cacheSend(mWaitResponses.front().responseBinary);
        }
        else
        {
            std::shared_ptr<std::string> errorReply = nullptr;
            int64_t num = 0;

            for (auto& v : mWaitResponses)
            {
                int64_t tmp;
                if (read_int64(v.ssdbReply, &tmp).ok())
                {
                    num += tmp;
                }
                else
                {
                    errorReply = v.responseBinary;
                    break;
                }
            }

            if (errorReply != nullptr)
            {
                client->cacheSend(errorReply);
            }
            else
            {
                static SSDBProtocolRequest response;
                response.init();

                response.writev(SSDB_OK, num);
                response.endl();
                client->cacheSend(response.getResult(), response.getResultLen());
            }
        }
    }
}

SSDBMultiGetWaitReply::SSDBMultiGetWaitReply(ClientLogicSession* client) : BaseWaitReply(client)
{
}

void SSDBMultiGetWaitReply::onBackendReply(int64_t dbServerSocketID, BackendParseMsg& msg)
{
    for (auto& v : mWaitResponses)
    {
        if (v.dbServerSocketID == dbServerSocketID)
        {
            v.responseBinary = msg.transfer();

            if (mWaitResponses.size() != 1)
            {
                v.ssdbReply = new SSDBProtocolResponse;
                v.ssdbReply->parse(v.responseBinary->c_str());
            }

            break;
        }
    }
}

void SSDBMultiGetWaitReply::mergeAndSend(ClientLogicSession* client)
{
    if (mErrorCode != nullptr)
    {
        syncSSDBStrList(client, { SSDB_ERROR, *mErrorCode });
    }
    else
    {
        if (mWaitResponses.size() == 1)
        {
            client->cacheSend(mWaitResponses.front().responseBinary);
        }
        else
        {
            std::shared_ptr<std::string> errorReply = nullptr;

            static vector<Bytes> kvs;
            kvs.clear();

            for (auto& v : mWaitResponses)
            {
                if (!read_bytes(v.ssdbReply, &kvs).ok())
                {
                    errorReply = v.responseBinary;
                    break;
                }
            }

            if (errorReply != nullptr)
            {
                client->cacheSend(errorReply);
            }
            else
            {
                static SSDBProtocolRequest strsResponse;
                strsResponse.init();

                strsResponse.writev(SSDB_OK);
                for (auto& v : kvs)
                {
                    strsResponse.appendStr(v.buffer, v.len);
                }
                
                strsResponse.endl();
                client->cacheSend(strsResponse.getResult(), strsResponse.getResultLen());
            }
        }
    }
}