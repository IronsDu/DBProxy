#include <fstream>

#include "RedisParse.h"
#include "Client.h"
#include "SSDBProtocol.h"
#include "SSDBWaitReply.h"

#include "Backend.h"

std::vector<BackendLogicSession*>    gBackendClients;

BackendExtNetSession::BackendExtNetSession(std::shared_ptr<BackendLogicSession> logicSession) : ExtNetSession(logicSession), mLogicSession(logicSession)
{
    cout << "BackendExtNetSession::BackendExtNetSession" << endl;
    mRedisParse = nullptr;
    mCache = nullptr;
}

BackendExtNetSession::~BackendExtNetSession()
{
    cout << "BackendExtNetSession::~BackendExtNetSession" << endl;
    if (mRedisParse != nullptr)
    {
        parse_tree_del(mRedisParse);
        mRedisParse = nullptr;
    }
}

/*  收到db server的reply，解析并放入逻辑消息队列   */
int BackendExtNetSession::onMsg(const char* buffer, int len)
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

void BackendExtNetSession::processReply(parse_tree* redisReply, std::shared_ptr<std::string>& responseBinary, const char* replyBuffer, size_t replyLen)
{
#ifdef PROXY_SINGLE_THREAD
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
    mLogicSession->onReply(tmp);
#else
    BackendParseMsg tmp;
    tmp.redisReply = redisReply;
    tmp.responseMemory = new std::shared_ptr<std::string>;
    if (responseBinary == nullptr)
    {
        tmp.responseMemory->reset(new std::string(replyBuffer, replyLen));
    }
    else
    {
        *tmp.responseMemory = responseBinary;
    }
    pushDataMsgToLogicThread((const char*)&tmp, sizeof(tmp));
#endif
}

void BackendLogicSession::onEnter() 
{
    gBackendClients.push_back(this);
}

void BackendLogicSession::onClose()
{
    for (auto it = gBackendClients.begin(); it != gBackendClients.end(); ++it)
    {
        if (*it == this)
        {
            gBackendClients.erase(it);
            break;
        }
    }
    
    /*  当与db server断开后，对等待此服务器响应的客户端请求设置错误(返回给客户端)  */
    while (!mPendingWaitReply.empty())
    {
        ClientLogicSession* client = nullptr;
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
            client->processCompletedReply();
        }
    }
}

BackendLogicSession::BackendLogicSession()
{
    cout << "BackendLogicSession::BackendLogicSession()" << endl;
}

BackendLogicSession::~BackendLogicSession()
{
    cout << "BackendLogicSession::~BackendLogicSession" << endl;
}

void BackendLogicSession::pushPendingWaitReply(std::weak_ptr<BaseWaitReply> w)
{
    mPendingWaitReply.push(w);
}

void BackendLogicSession::setID(int id)
{
    mID = id;
}

int BackendLogicSession::getID() const
{
    return mID;
}

void BackendLogicSession::onReply(BackendParseMsg& netParseMsg)
{
    if (!mPendingWaitReply.empty())
    {
        ClientLogicSession* client = nullptr;
        auto& replyPtr = mPendingWaitReply.front();
        auto reply = replyPtr.lock();
        if (reply != nullptr)
        {
            reply->onBackendReply(getSocketID(), netParseMsg);
            client = reply->getClient();
        }
        mPendingWaitReply.pop();
        if (client != nullptr)
        {
            client->processCompletedReply();
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

/*  收到网络层发送过来的db reply  */
void BackendLogicSession::onMsg(const char* buffer, int len)
{
    BackendParseMsg* netParseMsg = (BackendParseMsg*)buffer;
    onReply(*netParseMsg);
}

BackendLogicSession* findBackendByID(int id)
{
    BackendLogicSession* ret = nullptr;
    for (auto& v : gBackendClients)
    {
        if (v->getID() == id)
        {
            ret = v;
            break;
        }
    }

    return ret;
}