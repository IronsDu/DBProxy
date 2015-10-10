#include <fstream>

#include "RedisParse.h"
#include "Client.h"
#include "SSDBProtocol.h"
#include "SSDBWaitReply.h"

#include "Backend.h"

std::vector<BackendLogicSession*>    gBackendClients;

BackendExtNetSession::BackendExtNetSession(BaseLogicSession::PTR logicSession) : ExtNetSession(logicSession)
{
    cout << "建立到服务器的网络链接" << endl;
    mRedisParse = nullptr;
    mCache = nullptr;
}

BackendExtNetSession::~BackendExtNetSession()
{
    cout << "断开与服务器的网络链接" << endl;
    if (mRedisParse != nullptr)
    {
        parse_tree_del(mRedisParse);
        mRedisParse = nullptr;
    }
    if (mCache != nullptr)
    {
        delete mCache;
        mCache = nullptr;
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
                    BackendParseMsg tmp;
                    tmp.responseBinary = new string(parseStartPos, parseEndPos - parseStartPos);
                    tmp.redisReply = mRedisParse;
                    pushDataMsgToLogicThread((const char*)&tmp, sizeof(tmp));
                }
                else
                {
                    mCache->append(parseStartPos, parseEndPos - parseStartPos);

                    BackendParseMsg tmp;
                    tmp.responseBinary = mCache;
                    tmp.redisReply = mRedisParse;
                    pushDataMsgToLogicThread((const char*)&tmp, sizeof(tmp));

                    mCache = nullptr;
                }

                parseStartPos = parseEndPos;
                mRedisParse = nullptr;
            }
            else if (parseRet == REDIS_RETRY)
            {
                if (mCache == nullptr)
                {
                    mCache = new std::string;
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
            BackendParseMsg tmp;
            tmp.responseBinary = new string(parseStartPos, packetLen);
            pushDataMsgToLogicThread((const char*)&tmp, sizeof(tmp));

            totalLen += packetLen;
            leftLen -= packetLen;
            parseStartPos += packetLen;
        }
    }

    return totalLen;
}

void BackendLogicSession::onEnter() 
{
    cout << "建立与数据服务器的逻辑链接" << endl;
    gBackendClients.push_back(this);
}

void BackendLogicSession::onClose()
{
    cout << "断开与数据服务器的逻辑链接" << endl;
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

/*  收到网络层发送过来的db reply  */
void BackendLogicSession::onMsg(const char* buffer, int len)
{
    if (!mPendingWaitReply.empty())
    {
        ClientLogicSession* client = nullptr;
        auto& replyPtr = mPendingWaitReply.front();
        auto reply = replyPtr.lock();
        if (reply != nullptr)
        {
            BackendParseMsg* netParseMsg = (BackendParseMsg*)buffer;
            reply->onBackendReply(getSocketID(), *netParseMsg);
            if (netParseMsg->responseBinary != nullptr)
            {
                delete netParseMsg->responseBinary;
                netParseMsg->responseBinary = nullptr;
            }
            if (netParseMsg->redisReply != nullptr)
            {
                parse_tree_del(netParseMsg->redisReply);
                netParseMsg->redisReply = nullptr;
            }

            client = reply->getClient();
        }
        mPendingWaitReply.pop();
        if (client != nullptr)
        {
            client->processCompletedReply();
        }
    }
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