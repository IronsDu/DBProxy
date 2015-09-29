#include "SSDBProtocol.h"
#include "BaseWaitReply.h"

BaseWaitReply::BaseWaitReply(ClientLogicSession* client) : mClient(client), mErrorCode(nullptr)
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

    if (mErrorCode != nullptr)
    {
        delete mErrorCode;
        mErrorCode = nullptr;
    }
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
        if (v.reply == nullptr)
        {
            ret = false;
            break;
        }
    }

    return ret;
}