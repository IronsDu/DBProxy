#include <fstream>
#include <memory>
#include <iostream>

#include "protocol/RedisParse.h"
#include "Client.h"
#include "protocol/SSDBProtocol.h"
#include "SSDBWaitReply.h"

#include "Backend.h"

using namespace std;

class ClientSession;
std::vector<shared_ptr<BackendSession>>    gBackendClients;
std::mutex gBackendClientsLock;

BackendSession::BackendSession()
{
    cout << "BackendSession::BackendSession" << endl;
    mRedisParse = nullptr;
    mCache = nullptr;
}

BackendSession::~BackendSession()
{
    cout << "BackendSession::~BackendSession" << endl;
    if (mRedisParse != nullptr)
    {
        parse_tree_del(mRedisParse);
        mRedisParse = nullptr;
    }
}

void BackendSession::onEnter()
{
    gBackendClientsLock.lock();
    gBackendClients.push_back(shared_from_this());
    gBackendClientsLock.unlock();
}

void BackendSession::onClose()
{
    gBackendClientsLock.lock();

    for (auto it = gBackendClients.begin(); it != gBackendClients.end(); ++it)
    {
        if ((*it).get() == this)
        {
            gBackendClients.erase(it);
            break;
        }
    }

    /*  当与db server断开后，对等待此服务器响应的客户端请求设置错误(返回给客户端)  */
    while (!mPendingWaitReply.empty())
    {
        std::shared_ptr<ClientSession> client = nullptr;
        auto wp = mPendingWaitReply.front().lock();
        if (wp != nullptr)
        {
            wp->lockReply();
            wp->setError("backend error");
            wp->unLockReply();
            client = wp->getClient();
        }
        mPendingWaitReply.pop();
        if (client != nullptr)
        {
            if (client->getEventLoop()->isInLoopThread())
            {
                client->processCompletedReply();
            }
            else
            {
                client->getEventLoop()->pushAsyncProc([client](){
                    client->processCompletedReply();
                });
            }
        }
    }

    gBackendClientsLock.unlock();
}

/*  收到db server的reply，解析并放入逻辑消息队列   */
size_t BackendSession::onMsg(const char* buffer, size_t len)
{
    size_t totalLen = 0;

    const char c = buffer[0];
    if (mRedisParse != nullptr ||
        !IS_NUM(c))
    {
        /*  redis reply */
        char* parseEndPos = (char*)buffer;
        char* parseStartPos = parseEndPos;

        while (totalLen < len)
        {
            if (mRedisParse == nullptr)
            {
                mRedisParse = parse_tree_new();
            }

            int parseRet = parse(mRedisParse, &parseEndPos, (char*)buffer+len);
            totalLen += (parseEndPos - parseStartPos);

            if (parseRet == REDIS_OK)
            {
                if (mCache == nullptr)
                {
                    processReply(mRedisParse, mCache, parseStartPos, parseEndPos - parseStartPos);
                }
                else
                {
                    mCache->append(parseStartPos, parseEndPos - parseStartPos);
                    processReply(mRedisParse, mCache, mCache->c_str(), mCache->size());
                    mCache = nullptr;
                }

                parseStartPos = parseEndPos;
                mRedisParse = nullptr;
            }
            else if (parseRet == REDIS_RETRY)
            {
                if (mCache == nullptr)
                {
                    mCache.reset(new std::string);
                }
                mCache->append(parseStartPos, parseEndPos - parseStartPos);
                break;
            }
            else
            {
                break;
            }
        }
    }
    else
    {
        /*  ssdb reply    */
        char* parseStartPos = (char*)buffer;
        int leftLen = len;
        int packetLen = 0;
        while ((packetLen = SSDBProtocolResponse::check_ssdb_packet(parseStartPos, leftLen)) > 0)
        {
            processReply(nullptr, mCache, parseStartPos, packetLen);

            totalLen += packetLen;
            leftLen -= packetLen;
            parseStartPos += packetLen;
        }
    }

    return totalLen;
}

void BackendSession::processReply(parse_tree* redisReply, std::shared_ptr<std::string>& responseBinary, const char* replyBuffer, size_t replyLen)
{
    BackendParseMsg netParseMsg;
    netParseMsg.redisReply = redisReply;
    if (responseBinary != nullptr)
    {
        netParseMsg.responseMemory = responseBinary;
    }
    else
    {
        netParseMsg.responseMemory.reset(new std::string(replyBuffer, replyLen));
    }

    if (!mPendingWaitReply.empty())
    {
        std::shared_ptr<ClientSession> client = nullptr;
        auto reply = mPendingWaitReply.front().lock();
        mPendingWaitReply.pop();

        if (reply != nullptr)
        {
            /*  TODO::考虑将onBackendReply放入下面的pushAsyncProc回调中处理,那么这个加锁可完全去掉(但必须处理好BackendParseMsg资源!)    */
            reply->lockReply();
            if (netParseMsg.redisReply != nullptr && netParseMsg.redisReply->type == REDIS_REPLY_ERROR)
            {
                reply->setError(netParseMsg.redisReply->reply->str);
            }
            //ssdb会在mergeAndSend时处理错误/失败的response
            reply->onBackendReply(getSocketID(), netParseMsg);
            reply->unLockReply();
            client = reply->getClient();
        }

        if (client != nullptr)
        {
            auto eventLoop = client->getEventLoop();
            if (eventLoop->isInLoopThread())
            {
                client->processCompletedReply();
            }
            else
            {
                eventLoop->pushAsyncProc([client](){
                    client->processCompletedReply();
                });
            }
        }
    }
    else
    {
        assert(false);
    }

    if (netParseMsg.redisReply != nullptr)
    {
        parse_tree_del(netParseMsg.redisReply);
        netParseMsg.redisReply = nullptr;
    }
}

void BackendSession::forward(std::shared_ptr<BaseWaitReply>& waitReply, std::shared_ptr<string>& sharedStr, const char* b, size_t len)
{
    auto tmpSharedStr = sharedStr;
    if (tmpSharedStr == nullptr)
    {
        tmpSharedStr = std::make_shared<std::string>(b, len);
    }

    waitReply->lockReply();
    waitReply->addWaitServer(getSocketID());
    waitReply->unLockReply();

    auto sharedThis = shared_from_this();
    getEventLoop()->pushAsyncProc([sharedThis, waitReply, tmpSharedStr](){
        sharedThis->mPendingWaitReply.push(waitReply);
        sharedThis->sendPacket(tmpSharedStr);
    });
}

void BackendSession::forward(std::shared_ptr<BaseWaitReply>& waitReply, std::shared_ptr<std::string>&& sharedStr, const char* b, size_t len)
{
    forward(waitReply, sharedStr, b, len);
}

void BackendSession::setID(int id)
{
    mID = id;
}

int BackendSession::getID() const
{
    return mID;
}

shared_ptr<BackendSession> findBackendByID(int id)
{
    shared_ptr<BackendSession> ret = nullptr;

    gBackendClientsLock.lock();
    for (auto& v : gBackendClients)
    {
        if (v->getID() == id)
        {
            ret = v;
            break;
        }
    }
    gBackendClientsLock.unlock();

    return ret;
}