#include "protocol/SSDBProtocol.h"
#include "protocol/RedisParse.h"

#include "BaseWaitReply.h"

BaseWaitReply::BaseWaitReply(const ClientSession::PTR& client) : mClient(client)
{}

BaseWaitReply::~BaseWaitReply()
{
}

const std::shared_ptr<ClientSession>& BaseWaitReply::getClient() const
{
    return mClient;
}

void BaseWaitReply::addWaitServer(int64_t serverSocketID)
{
    PendingResponseStatus tmp;
    tmp.dbServerSocketID = serverSocketID;
    mWaitResponses.push_back(tmp);
}

void BaseWaitReply::setError(const char* errorCode)
{
    mErrorCode = std::string(errorCode);
}

bool BaseWaitReply::hasError() const
{
    return !mErrorCode.empty();
}

bool BaseWaitReply::isAllCompleted() const
{
    bool ret = true;

    for (auto& v : mWaitResponses)
    {
        if (v.forceOK == false && v.redisReply == nullptr && v.ssdbReply == nullptr && v.responseBinary == nullptr)
        {
            ret = false;
            break;
        }
    }

    return ret;
}