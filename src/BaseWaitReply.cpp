#include "SSDBProtocol.h"
#include "BaseWaitReply.h"

BaseWaitReply::BaseWaitReply(ClientLogicSession* client) : mClient(client), mIsError(false)
{}

BaseWaitReply::~BaseWaitReply()
{
    for (auto& v : mWaitResponses)
    {
        if (v.reply != nullptr)
        {
            delete v.reply;
            v.reply = nullptr;
        }
        if (v.ssdbReply != nullptr)
        {
            delete v.ssdbReply;
            v.ssdbReply = nullptr;
        }
    }

    mWaitResponses.clear();
}

ClientLogicSession* BaseWaitReply::getClient()
{
    return mClient;
}

void BaseWaitReply::addWaitServer(int64_t serverSocketID)
{
    PendingResponseStatus tmp;
    tmp.dbServerSocketID = serverSocketID;
    mWaitResponses.push_back(tmp);
}

void BaseWaitReply::setError()
{
    mIsError = true;
}

bool BaseWaitReply::hasError() const
{
    return mIsError;
}

bool BaseWaitReply::isAllCompleted() const
{
    bool ret = true;

    for (auto& v : mWaitResponses)
    {
        if (v.reply == nullptr)
        {
            ret = false;
            break;
        }
    }

    return ret;
}