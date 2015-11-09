#include <fstream>
#include <memory>

#include "RedisParse.h"
#include "Client.h"
#include "SSDBProtocol.h"
#include "SSDBWaitReply.h"

#include "Backend.h"

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
        auto& w = mPendingWaitReply.front();
        auto wp = w.lock();
        if (wp != nullptr)
        {
            wp->setError("backend error");
            client = wp->getClient();
        }
        mPendingWaitReply.pop();
        if (client != nullptr)
        {
            client->getEventLoop()->pushAsyncProc([client](){
                client->processCompletedReply();
            });
        }
    }


    gBackendClientsLock.unlock();
}

/*  收到db server的reply，解析并放入逻辑消息队列   */
int BackendSession::onMsg(const char* buffer, int len)
{
    int totalLen = 0;

    const char c = buffer[0];
    if (mRedisParse != nullptr ||
        !IS_NUM(c))
    {
        /*  redis reply */
        char* parseEndPos = (char*)buffer;
        char* parseStartPos = parseEndPos;
        string lastPacket;
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
            processReply(mRedisParse, mCache, parseStartPos, packetLen);

            totalLen += packetLen;
            leftLen -= packetLen;
            parseStartPos += packetLen;
        }
    }

    return totalLen;
}

void BackendSession::processReply(parse_tree* redisReply, std::shared_ptr<std::string>& responseBinary, const char* replyBuffer, size_t replyLen)
{
    BackendParseMsg tmp;
    tmp.redisReply = redisReply;
    tmp.responseBuffer = replyBuffer;
    tmp.responseLen = replyLen;
    tmp.responseMemory = new std::shared_ptr< std::string >;   /*todo,避免构造智能指针*/
    if (responseBinary != nullptr)
    {
        *tmp.responseMemory = responseBinary;
    }
    else
    {
        tmp.responseMemory->reset(new std::string(replyBuffer, replyLen));
    }

    onReply(tmp);
}

void BackendSession::forward(std::shared_ptr<BaseWaitReply>& w, std::shared_ptr<string>& r, const char* b, size_t len)
{
    w->addWaitServer(getSocketID());
    mPendingListLock.lock();
    mPendingWaitReply.push(w);
    if (r != nullptr)
    {
        sendPacket(r);
    }
    else
    {
        sendPacket(b, len);
    }
    mPendingListLock.unlock();
}

void BackendSession::forward(std::shared_ptr<BaseWaitReply>& w, std::shared_ptr<string>&& r, const char* b, size_t len)
{
    forward(w, r, b, len);
}

void BackendSession::setID(int id)
{
    mID = id;
}

int BackendSession::getID() const
{
    return mID;
}

void BackendSession::onReply(BackendParseMsg& netParseMsg)
{
    if (!mPendingWaitReply.empty())
    {
        std::shared_ptr<ClientSession> client = nullptr;
        std::shared_ptr<BaseWaitReply> reply = nullptr;

        mPendingListLock.lock();
        reply = mPendingWaitReply.front().lock();
        mPendingWaitReply.pop();
        mPendingListLock.unlock();

        if (reply != nullptr)
        {
            reply->onBackendReply(getSocketID(), netParseMsg);
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
    if (netParseMsg.responseMemory != nullptr)
    {
        delete netParseMsg.responseMemory;
        netParseMsg.responseMemory = nullptr;
    }
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