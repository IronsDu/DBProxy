#include "SSDBProtocol.h"
#include "RedisParse.h"

#include "BaseWaitReply.h"

BaseWaitReply::BaseWaitReply(std::shared_ptr<ClientSession>& client) : mClient(client), mErrorCode(nullptr)
{}

BaseWaitReply::~BaseWaitReply()
{
    for (auto& v : mWaitResponses)
    {
        if (v.ssdbReply != nullptr)
        {
            delete v.ssdbReply;
            v.ssdbReply = nullptr;
        }
        if (v.redisReply != nullptr)
        {
            parse_tree_del(v.redisReply);
            v.redisReply = nullptr;
        }
    }

    mWaitResponses.clear();

    if (mErrorCode != nullptr)
    {
        delete mErrorCode;
        mErrorCode = nullptr;
    }
}

std::shared_ptr<ClientSession>& BaseWaitReply::getClient()
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
    mErrorCode = new std::string(errorCode);
}

bool BaseWaitReply::hasError() const
{
    return mErrorCode != nullptr;
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