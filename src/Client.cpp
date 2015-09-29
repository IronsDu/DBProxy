#include "RedisParse.h"
#include "Backend.h"
#include "SSDBProtocol.h"
#include "SSDBWaitReply.h"
#include "RedisWaitReply.h"

#include "Client.h"

ClientExtNetSession::ClientExtNetSession(BaseLogicSession::PTR logicSession) : ExtNetSession(logicSession)
{
    mRedisParse = nullptr;
}

ClientExtNetSession::~ClientExtNetSession()
{
    if (mRedisParse != nullptr)
    {
        parse_tree_del(mRedisParse);
        mRedisParse = nullptr;
    }
}

struct ParseMsg
{
    ParseMsg()
    {
        ssdbQuery = nullptr;
        msg = nullptr;
        buffer = nullptr;
    }

    SSDBProtocolResponse* ssdbQuery;
    parse_tree* msg;
    string* buffer;
};

/*  收到客户端的请求,并解析请求投入到逻辑消息队列    */
int ClientExtNetSession::onMsg(const char* buffer, int len)
{
    int totalLen = 0;

    const char h = buffer[0];
    if (mRedisParse != nullptr ||
        !IS_NUM(h))
    {
        /*TODO::处理非完成的服务器操作相关命令--非数据操作相关的命令协议*/
        if (strncmp(buffer, "PING\r\n", 6) ==0)
        {
            ParseMsg tmp;
            tmp.buffer = new string(buffer, 6);
            pushDataMsgToLogicThread((const char*)&tmp, sizeof(tmp));
            return 6;
        }

        /*  redis request   */
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
                ParseMsg tmp;
                tmp.msg = mRedisParse;

                if (mCache.empty())
                {
                    tmp.buffer = new string(parseStartPos, parseEndPos - parseStartPos);
                }
                else
                {
                    mCache.append(parseStartPos, parseEndPos - parseStartPos);
                    tmp.buffer = new string(std::move(mCache));
                    mCache.clear();
                }
                parseStartPos = parseEndPos;
                pushDataMsgToLogicThread((const char*)&tmp, sizeof(tmp));
                mRedisParse = parse_tree_new();
            }
            else if (parseRet == REDIS_RETRY)
            {
                mCache.append(parseStartPos, parseEndPos - parseStartPos);
                break;
            }
            else
            {
                assert(false);
                break;
            }
        }
    }
    else
    {
        /*  ssdb request    */
        char* parseStartPos = (char*)buffer;
        int leftLen = len;
        int packetLen = 0;
        while ((packetLen = SSDBProtocolResponse::check_ssdb_packet(parseStartPos, leftLen)) > 0)
        {
            ParseMsg tmp;
            tmp.buffer = new string(parseStartPos, packetLen);
            tmp.ssdbQuery = new SSDBProtocolResponse;
            tmp.ssdbQuery->parse(tmp.buffer->c_str());

            pushDataMsgToLogicThread((const char*)&tmp, sizeof(tmp));

            totalLen += packetLen;
            leftLen -= packetLen;
            parseStartPos += packetLen;
        }
    }

    return totalLen;
}

ClientLogicSession::ClientLogicSession()
{
    mNeedAuth = false;
    mIsAuth = false;
}

void ClientLogicSession::onEnter()
{

}

void ClientLogicSession::onClose()
{

}

extern bool sharding_key(const char* str, int len, int& serverID);

void ClientLogicSession::pushSSDBStrListReply(const std::vector< const char*> &strlist)
{
    std::shared_ptr<StrListSSDBReply> reply = std::make_shared<StrListSSDBReply>(this);
    for (auto& v : strlist)
    {
        reply->pushStr(v);
    }
    mPendingReply.push_back(reply);
    processCompletedReply();
}

void ClientLogicSession::pushSSDBErrorReply(const char* error)
{
    pushSSDBStrListReply({ "error", error });
}

void ClientLogicSession::pushRedisErrorReply(const char* error)
{
    BaseWaitReply::PTR w = std::make_shared<RedisErrorReply>(this, error);
    mPendingReply.push_back(w);
    processCompletedReply();
}

void ClientLogicSession::pushRedisStatusReply(const char* status)
{
    BaseWaitReply::PTR w = std::make_shared<RedisStatusReply>(this, status);
    mPendingReply.push_back(w);
    processCompletedReply();
}

void ClientLogicSession::onMsg(const char* buffer, int len)
{
    assert(sizeof(ParseMsg) == len);
    ParseMsg* msg = (ParseMsg*)buffer;

    if (msg->ssdbQuery != nullptr)
    {
        /*  处理ssdb 命令   */
        bool isSuccess = false;

        if (!gBackendClients.empty())
        {
            Bytes* op = msg->ssdbQuery->getByIndex(0);
            if (op != nullptr)
            {
                BaseWaitReply::PTR w = nullptr;
                if (strncmp("auth", op->buffer, op->len) == 0)
                {
                    isSuccess = procSSDBAuth(msg->ssdbQuery, msg->buffer);
                }
                else
                {
                    if (mNeedAuth && !mIsAuth)
                    {
                        pushSSDBStrListReply({ "noauth", "authentication required" });
                        isSuccess = true;
                    }
                    else if (strncmp("ping", op->buffer, op->len) == 0)
                    {
                        isSuccess = procSSDBPing(msg->ssdbQuery, msg->buffer);
                    }
                    else if (strncmp("multi_set", op->buffer, op->len) == 0)
                    {
                        isSuccess = procSSDBMultiSet(msg->ssdbQuery, msg->buffer);
                    }
                    else if (strncmp("multi_get", op->buffer, op->len) == 0)
                    {
                        isSuccess = procSSDBCommandOfMultiKeys(std::make_shared<SSDBMultiGetWaitReply>(this), msg->ssdbQuery, msg->buffer, "multi_get");
                    }
                    else if (strncmp("multi_del", op->buffer, op->len) == 0)
                    {
                        isSuccess = procSSDBCommandOfMultiKeys(std::make_shared<SSDBMultiDelWaitReply>(this), msg->ssdbQuery, msg->buffer, "multi_del");
                    }
                    else
                    {
                        isSuccess = procSSDBSingleCommand(msg->ssdbQuery, msg->buffer);
                    }
                }
            }
        }

        if (!isSuccess)
        {
            /*  模拟一个错误  */
            pushSSDBErrorReply("command not process");
        }
    }
    else
    {
        /*TODO::重用网络层分包的命令判断，减少此处的字符串比较*/
        if (strncmp(msg->buffer->c_str(), "PING\r\n", 6) == 0)
        {
            pushRedisStatusReply("PONG");
        }
        else
        {
            /*  处理redis命令, TODO::处理mset、mget、del   */
            bool isSuccess = processRedisSingleCommand(msg->msg, msg->buffer);
            if (!isSuccess)
            {
                /*  模拟一个错误  */
                pushRedisErrorReply("no error for key");
            }
        }
    }

    if (msg->ssdbQuery != nullptr)
    {
        delete msg->ssdbQuery;
        msg->ssdbQuery = nullptr;
    }
    if (msg->msg != nullptr)
    {
        parse_tree_del(msg->msg);
        msg->msg = nullptr;
    }
    delete msg->buffer;
}

bool ClientLogicSession::procSSDBAuth(SSDBProtocolResponse* request, std::string* requestStr)
{
    if (request->getBuffersLen() == 2)
    {
        Bytes* p = request->getByIndex(1);
        if (!mNeedAuth || strncmp(p->buffer, mPassword.c_str(), p->len) == 0)
        {
            mIsAuth = true;
            pushSSDBStrListReply({ "ok" });
        }
        else
        {
            pushSSDBErrorReply("invalid password");
        }
    }
    else
    {
        pushSSDBStrListReply({ "client_error" });
    }

    return true;
}

bool ClientLogicSession::procSSDBPing(SSDBProtocolResponse* , std::string* requestStr)
{
    pushSSDBStrListReply({ "ok" });
    return true;
}

bool ClientLogicSession::procSSDBMultiSet(SSDBProtocolResponse* request, std::string* requestStr)
{
    bool isSuccess = (request->getBuffersLen() - 1) % 2 == 0 && request->getBuffersLen() > 1;

    BaseWaitReply::PTR w = std::make_shared<SSDBMultiSetWaitReply>(this);
    unordered_map<int, std::vector<Bytes>> kvsMap;

    if (isSuccess)
    {
        for (size_t i = 1; i < request->getBuffersLen(); i += 2)
        {
            Bytes* b = request->getByIndex(i);
            int serverID;
            if (sharding_key(b->buffer, b->len, serverID))
            {
                auto it = kvsMap.find(serverID);
                if (it == kvsMap.end())
                {
                    std::vector<Bytes> tmp;
                    tmp.push_back(*b);
                    tmp.push_back(*(request->getByIndex(i + 1)));
                    kvsMap[serverID] = std::move(tmp);
                }
                else
                {
                    (*it).second.push_back(*b);
                    (*it).second.push_back(*(request->getByIndex(i + 1)));
                }
            }
            else
            {
                isSuccess = false;
                break;
            }
        }
    }

    if (isSuccess)
    {
        if (kvsMap.size() == 1)
        {
            BackendLogicSession* server = findBackendByID((*kvsMap.begin()).first);

            w->addWaitServer(server->getSocketID());
            server->pushPendingWaitReply(w);
            server->send(requestStr->c_str(), requestStr->size());
        }
        else
        {
            SSDBProtocolRequest request2Backend;

            for (auto& v : kvsMap)
            {
                request2Backend.init();
                BackendLogicSession* server = findBackendByID(v.first);

                request2Backend.appendStr("multi_set");
                for (auto& k : v.second)
                {
                    request2Backend.appendStr(k.buffer, k.len);
                }
                request2Backend.endl();

                w->addWaitServer(server->getSocketID());
                server->pushPendingWaitReply(w);
                server->send(request2Backend.getResult(), request2Backend.getResultLen());
            }
        }

        mPendingReply.push_back(w);
    }

    return isSuccess;
}

bool ClientLogicSession::procSSDBCommandOfMultiKeys(std::shared_ptr<BaseWaitReply> w, SSDBProtocolResponse* request, std::string* requestStr, const char* command)
{
    bool isSuccess = request->getBuffersLen() > 1;

    unordered_map<int, std::vector<Bytes>> serverKs;

    for (size_t i = 1; i < request->getBuffersLen(); ++i)
    {
        Bytes* b = request->getByIndex(i);
        int serverID;
        if (sharding_key(b->buffer, b->len, serverID))
        {
            auto it = serverKs.find(serverID);
            if (it == serverKs.end())
            {
                std::vector<Bytes> tmp;
                tmp.push_back(*b);
                serverKs[serverID] = std::move(tmp);
            }
            else
            {
                (*it).second.push_back(*b);
            }
        }
        else
        {
            isSuccess = false;
            break;
        }
    }

    if (isSuccess)
    {
        if (serverKs.size() == 1)
        {
            BackendLogicSession* server = findBackendByID((*serverKs.begin()).first);

            w->addWaitServer(server->getSocketID());
            server->pushPendingWaitReply(w);
            server->send(requestStr->c_str(), requestStr->size());
        }
        else
        {
            SSDBProtocolRequest request2Backend;

            for (auto& v : serverKs)
            {
                request2Backend.init();
                BackendLogicSession* server = findBackendByID(v.first);

                request2Backend.appendStr(command);
                for (auto& k : v.second)
                {
                    request2Backend.appendStr(k.buffer, k.len);
                }
                request2Backend.endl();

                w->addWaitServer(server->getSocketID());
                server->pushPendingWaitReply(w);
                server->send(request2Backend.getResult(), request2Backend.getResultLen());
            }
        }

        mPendingReply.push_back(w);
    }

    return isSuccess;
}

bool ClientLogicSession::procSSDBSingleCommand(SSDBProtocolResponse* request, std::string* requestStr)
{
    bool isSuccess = false;
    
    Bytes* b = request->getByIndex(1);
    int serverID;
    if (sharding_key(b->buffer, b->len, serverID))
    {
        isSuccess = true;
        BaseWaitReply::PTR w = std::make_shared<SSDBSingleWaitReply>(this);
        auto server = findBackendByID(serverID);
        w->addWaitServer(server->getSocketID());
        server->pushPendingWaitReply(w);
        server->send(requestStr->c_str(), requestStr->size());

        mPendingReply.push_back(w);
    }

    return isSuccess;
}

bool ClientLogicSession::processRedisSingleCommand(parse_tree* parse, std::string* requestStr)
{
    bool isSuccess = false;

    int serverID;
    if (sharding_key(parse->tmp_buff, strlen(parse->tmp_buff), serverID))
    {
        isSuccess = true;
        BaseWaitReply::PTR w = std::make_shared<RedisSingleWaitReply>(this);
        auto server = findBackendByID(serverID);
        w->addWaitServer(server->getSocketID());
        server->pushPendingWaitReply(w);
        server->send(requestStr->c_str(), requestStr->size());

        mPendingReply.push_back(w);
    }

    return isSuccess;
}

void ClientLogicSession::processCompletedReply()
{
    while (!mPendingReply.empty())
    {
        auto& w = mPendingReply.front();
        if (w->isAllCompleted() || w->hasError())
        {
            w->mergeAndSend(this);
            mPendingReply.pop_front();
        }
        else
        {
            break;
        }
    }
}