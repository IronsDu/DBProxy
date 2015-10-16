#include "RedisParse.h"
#include "Backend.h"
#include "SSDBProtocol.h"
#include "SSDBWaitReply.h"
#include "RedisWaitReply.h"
#include "RedisRequest.h"

#include "Client.h"

ClientExtNetSession::ClientExtNetSession(std::shared_ptr<ClientLogicSession> logicSession) : ExtNetSession(logicSession), mLogicSession(logicSession)
{
    cout << "ClientExtNetSession::ClientExtNetSession()" << endl;
    mRedisParse = nullptr;
}

ClientExtNetSession::~ClientExtNetSession()
{
    cout << "ClientExtNetSession::~ClientExtNetSession()" << endl;
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
        ssdbRequest = nullptr;
        redisRequest = nullptr;
        requestBinary = nullptr;
    }

    SSDBProtocolResponse* ssdbRequest;
    parse_tree* redisRequest;
    std::shared_ptr<std::string>*   requestBinary;
};

/*  收到客户端的请求,并解析请求投入到逻辑消息队列    */
int ClientExtNetSession::onMsg(const char* buffer, int len)
{
    int totalLen = 0;

    const char h = buffer[0];
    if (mRedisParse != nullptr ||
        !IS_NUM(h))
    {
        /*  redis request   */
        char* parseEndPos = (char*)buffer;
        char* parseStartPos = parseEndPos;
        while (totalLen < len)
        {
            if (mRedisParse == nullptr)
            {
                /*TODO::处理非完成的服务器操作相关命令--非数据操作相关的命令协议*/
                if (strncmp(parseStartPos, "PING\r\n", 6) == 0)
                {
                    processRequest(true, nullptr, nullptr, mCache, parseStartPos, 6);

                    totalLen += 6;
                    parseStartPos += 6;

                    continue;
                }
                else
                {
                    mRedisParse = parse_tree_new();
                }
            }

            int parseRet = parse(mRedisParse, &parseEndPos, (char*)buffer+len);
            totalLen += (parseEndPos - parseStartPos);

            if (parseRet == REDIS_OK)
            {
                if (mCache == nullptr)
                {
                    processRequest(true, nullptr, mRedisParse, mCache, parseStartPos, parseEndPos - parseStartPos);
                }
                else
                {
                    mCache->append(parseStartPos, parseEndPos - parseStartPos);
                    processRequest(true, nullptr, mRedisParse, mCache, mCache->c_str(), mCache->size());
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
#ifdef PROXY_SINGLE_THREAD
            SSDBProtocolResponse* ssdbQuery = new SSDBProtocolResponse;
            ssdbQuery->parse(parseStartPos);
            processRequest(false, ssdbQuery, nullptr, mCache, parseStartPos, packetLen);
#else
            processRequest(false, nullptr, nullptr, mCache, parseStartPos, packetLen);
#endif

            totalLen += packetLen;
            leftLen -= packetLen;
            parseStartPos += packetLen;
        }
    }

    return totalLen;
}

void ClientExtNetSession::processRequest(bool isRedis, SSDBProtocolResponse* ssdbQuery, parse_tree* redisRequest, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen)
{
#ifdef PROXY_SINGLE_THREAD
    mLogicSession->onRequest(isRedis, ssdbQuery, redisRequest, requestBinary, requestBuffer, requestLen);
    if (redisRequest != nullptr)
    {
        parse_tree_del(redisRequest);
        redisRequest = nullptr;
    }
    if (ssdbQuery != nullptr)
    {
        delete ssdbQuery;
        ssdbQuery = nullptr;
    }
#else
    ParseMsg tmp;
    tmp.requestBinary = new std::shared_ptr<std::string>;
    if (requestBinary != nullptr)
    {
        *tmp.requestBinary = requestBinary;
    }
    else
    {
        tmp.requestBinary->reset(new std::string(requestBuffer, requestLen));
    }

    if (!isRedis)
    {
        tmp.ssdbRequest = new SSDBProtocolResponse;
        tmp.ssdbRequest->parse(tmp.requestBinary->get()->c_str());
    }
    tmp.redisRequest = redisRequest;
    pushDataMsgToLogicThread((const char*)&tmp, sizeof(tmp));
#endif
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

void ClientLogicSession::onRequest(bool isRedis, SSDBProtocolResponse* ssdbQuery, parse_tree* redisRequest, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen)
{
    if (!isRedis)
    {
        /*  处理ssdb 命令   */
        bool isSuccess = false;

        if (!gBackendClients.empty())
        {
            Bytes* op = ssdbQuery->getByIndex(0);
            if (op != nullptr)
            {
                BaseWaitReply::PTR w = nullptr;
                if (strncmp("auth", op->buffer, op->len) == 0)
                {
                    isSuccess = procSSDBAuth(ssdbQuery, requestBuffer, requestLen);
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
                        isSuccess = procSSDBPing(ssdbQuery, requestBuffer, requestLen);
                    }
                    else if (strncmp("multi_set", op->buffer, op->len) == 0)
                    {
                        isSuccess = procSSDBMultiSet(ssdbQuery, requestBuffer, requestLen);
                    }
                    else if (strncmp("multi_get", op->buffer, op->len) == 0)
                    {
                        isSuccess = procSSDBCommandOfMultiKeys(std::make_shared<SSDBMultiGetWaitReply>(this), ssdbQuery, requestBuffer, requestLen, "multi_get");
                    }
                    else if (strncmp("multi_del", op->buffer, op->len) == 0)
                    {
                        isSuccess = procSSDBCommandOfMultiKeys(std::make_shared<SSDBMultiDelWaitReply>(this), ssdbQuery, requestBuffer, requestLen, "multi_del");
                    }
                    else
                    {
                        isSuccess = procSSDBSingleCommand(ssdbQuery, requestBuffer, requestLen);
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
        if (strncmp(requestBuffer, "PING\r\n", 6) == 0)
        {
            pushRedisStatusReply("PONG");
        }
        else
        {
            const char* op = redisRequest->reply->element[0]->str;
            const size_t oplen = redisRequest->reply->element[0]->len;

            bool isSuccess = false;

            if (strncmp(op, "PING", 4) == 0)
            {
                pushRedisStatusReply("PONG");
            }
            else if (strncmp(op, "mget", oplen) == 0)
            {
                isSuccess = processRedisCommandOfMultiKeys(std::make_shared<RedisMgetWaitReply>(this), redisRequest, requestBinary, requestBuffer, requestLen, "mget");
            }
            else if (strncmp(op, "mset", oplen) == 0)
            {
                isSuccess = processRedisMset(redisRequest, requestBinary, requestBuffer, requestLen);
            }
            else if (strncmp(op, "del", oplen) == 0)
            {
                isSuccess = processRedisCommandOfMultiKeys(std::make_shared<RedisDelWaitReply>(this), redisRequest, requestBinary, requestBuffer, requestLen, "del");
            }
            else
            {
                isSuccess = processRedisSingleCommand(redisRequest, requestBinary, requestBuffer, requestLen);
            }

            if (!isSuccess)
            {
                /*  模拟一个错误  */
                pushRedisErrorReply("no error for key");
            }
        }
    }
}

void ClientLogicSession::onMsg(const char* buffer, int len)
{
    assert(sizeof(ParseMsg) == len);
    ParseMsg* msg = (ParseMsg*)buffer;

    onRequest(msg->ssdbRequest == nullptr, msg->ssdbRequest, msg->redisRequest, *msg->requestBinary, 
              msg->requestBinary->get()->c_str(), msg->requestBinary->get()->size());

    if (msg->ssdbRequest != nullptr)
    {
        delete msg->ssdbRequest;
        msg->ssdbRequest = nullptr;
    }
    if (msg->redisRequest != nullptr)
    {
        parse_tree_del(msg->redisRequest);
        msg->redisRequest = nullptr;
    }
    if (msg->requestBinary != nullptr)
    {
        delete msg->requestBinary;
        msg->requestBinary = nullptr;
    }
}

bool ClientLogicSession::procSSDBAuth(SSDBProtocolResponse* request, const char* requestBuffer, size_t requestLen)
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

bool ClientLogicSession::procSSDBPing(SSDBProtocolResponse*, const char* requestBuffer, size_t requestLen)
{
    pushSSDBStrListReply({ "ok" });
    return true;
}

bool ClientLogicSession::procSSDBMultiSet(SSDBProtocolResponse* request, const char* requestBuffer, size_t requestLen)
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
            server->cacheSend(requestBuffer, requestLen);
        }
        else
        {
            static SSDBProtocolRequest request2Backend;

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
                server->cacheSend(request2Backend.getResult(), request2Backend.getResultLen());
            }
        }

        mPendingReply.push_back(w);
    }

    return isSuccess;
}

bool ClientLogicSession::procSSDBCommandOfMultiKeys(std::shared_ptr<BaseWaitReply> w, SSDBProtocolResponse* request, const char* requestBuffer, size_t requestLen, const char* command)
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
            server->cacheSend(requestBuffer, requestLen);
        }
        else
        {
            static SSDBProtocolRequest request2Backend;

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
                server->cacheSend(request2Backend.getResult(), request2Backend.getResultLen());
            }
        }

        mPendingReply.push_back(w);
    }

    return isSuccess;
}

bool ClientLogicSession::procSSDBSingleCommand(SSDBProtocolResponse* request, const char* requestBuffer, size_t requestLen)
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
        server->cacheSend(requestBuffer, requestLen);

        mPendingReply.push_back(w);
    }

    return isSuccess;
}

bool ClientLogicSession::processRedisSingleCommand(parse_tree* parse, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen)
{
    bool isSuccess = false;

    int serverID;
    if (sharding_key(parse->reply->element[1]->str, parse->reply->element[1]->len, serverID))
    {
        isSuccess = true;
        BaseWaitReply::PTR w = std::make_shared<RedisSingleWaitReply>(this);
        auto server = findBackendByID(serverID);
        w->addWaitServer(server->getSocketID());
        server->pushPendingWaitReply(w);
        if (requestBinary != nullptr)
        {
            server->cacheSend(requestBinary);
        }
        else
        {
            server->cacheSend(requestBuffer, requestLen);
        }

        mPendingReply.push_back(w);
    }

    return isSuccess;
}

bool ClientLogicSession::processRedisMset(parse_tree* parse, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen)
{
    bool isSuccess = parse->reply->elements > 1 && (parse->reply->elements-1) % 2 == 0;

    unordered_map<int, std::vector<Bytes>> serverKvs;

    for (size_t i = 1; i < parse->reply->elements; i+=2)
    {
        int serverID;

        const char* key = parse->reply->element[i]->str;
        int keyLen = parse->reply->element[i]->len;
        const char* value = parse->reply->element[i+1]->str;
        int valueLen = parse->reply->element[i+1]->len;

        if (sharding_key(key, keyLen, serverID))
        {
            auto it = serverKvs.find(serverID);
            if (it == serverKvs.end())
            {
                std::vector<Bytes> tmp;
                tmp.push_back({key, keyLen});
                tmp.push_back({value,valueLen});
                serverKvs[serverID] = std::move(tmp);
            }
            else
            {
                (*it).second.push_back({key, keyLen});
                (*it).second.push_back({value, valueLen});
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
        BaseWaitReply::PTR w = std::make_shared<RedisMsetWaitReply>(this);
        if (serverKvs.size() == 1)
        {
            BackendLogicSession* server = findBackendByID((*serverKvs.begin()).first);

            w->addWaitServer(server->getSocketID());
            server->pushPendingWaitReply(w);
            if (requestBinary != nullptr)
            {
                server->cacheSend(requestBinary);
            }
            else
            {
                server->cacheSend(requestBuffer, requestLen);
            }
        }
        else
        {
            static RedisProtocolRequest request2Backend;

            for (auto& v : serverKvs)
            {
                request2Backend.init();
                BackendLogicSession* server = findBackendByID(v.first);
                request2Backend.writev("mset");
                for (auto& k : v.second)
                {
                    request2Backend.appendBinary(k.buffer, k.len);
                }
                request2Backend.endl();

                w->addWaitServer(server->getSocketID());
                server->pushPendingWaitReply(w);
                server->cacheSend(request2Backend.getResult(), request2Backend.getResultLen());
            }
        }

        mPendingReply.push_back(w);
    }

    return isSuccess;
}

bool ClientLogicSession::processRedisCommandOfMultiKeys(std::shared_ptr<BaseWaitReply> w, parse_tree* parse, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen, const char* command)
{
    bool isSuccess = parse->reply->elements > 1;

    unordered_map<int, std::vector<Bytes>> serverKs;

    for (size_t i = 1; i < parse->reply->elements; ++i)
    {
        int serverID;
        const char* key = parse->reply->element[i]->str;
        int keyLen = parse->reply->element[i]->len;

        if (sharding_key(key, keyLen, serverID))
        {
            auto it = serverKs.find(serverID);
            if (it == serverKs.end())
            {
                std::vector<Bytes> tmp;
                tmp.push_back({key, keyLen});
                serverKs[serverID] = std::move(tmp);
            }
            else
            {
                (*it).second.push_back({key, keyLen});
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
            if (requestBinary != nullptr)
            {
                server->cacheSend(requestBinary);
            }
            else
            {
                server->cacheSend(requestBuffer, requestLen);
            }
        }
        else
        {
            static RedisProtocolRequest request2Backend;

            for (auto& v : serverKs)
            {
                request2Backend.init();
                BackendLogicSession* server = findBackendByID(v.first);
                request2Backend.appendBinary(command, strlen(command));
                for (auto& k : v.second)
                {
                    request2Backend.appendBinary(k.buffer, k.len);
                }
                request2Backend.endl();

                w->addWaitServer(server->getSocketID());
                server->pushPendingWaitReply(w);
                server->cacheSend(request2Backend.getResult(), request2Backend.getResultLen());
            }
        }

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
