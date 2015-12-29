#include "RedisParse.h"
#include "Backend.h"
#include "SSDBProtocol.h"
#include "SSDBWaitReply.h"
#include "RedisWaitReply.h"
#include "RedisRequest.h"

extern "C"
{
#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"
#include "luaconf.h"
};

#include "Client.h"

extern bool sharding_key(struct lua_State*, const char* str, int len, int& serverID);
extern struct lua_State* malloc_luaState();

ClientSession::ClientSession()
{
    mPendingReply = new std::deque < std::shared_ptr<BaseWaitReply> > ;
    cout << "ClientSession::ClientSession()" << endl;
    mRedisParse = nullptr;
    mLua = malloc_luaState();
    mNeedAuth = false;
    mIsAuth = false;
}

ClientSession::~ClientSession()
{
    cout << "ClientSession::~ClientSession()" << endl;

    if (mRedisParse != nullptr)
    {
        parse_tree_del(mRedisParse);
        mRedisParse = nullptr;
    }
    if (mLua != nullptr)
    {
        lua_close(mLua);
        mLua = nullptr;
    }
}

RedisProtocolRequest& ClientSession::getCacheRedisProtocol()
{
    return mCacheRedisProtocol;
}

SSDBProtocolRequest& ClientSession::getCacheSSDBProtocol()
{
    return mCacheSSDBProtocol;
}

struct ParseMsg
{
    ParseMsg()
    {
        ssdbRequest = nullptr;
        redisRequest = nullptr;
        requestBinary = nullptr;
    }

    SSDBProtocolResponse* ssdbRequest;  /*解析出的ssdb request协议*/
    parse_tree* redisRequest;           /*解析出的redis request协议*/
    std::shared_ptr<std::string>*   requestBinary; /*原始的协议报文(用于可直接转发给某backend server时，避免重新构造request报文*/
};

void ClientSession::onEnter()
{
}

void ClientSession::onClose()
{
}

/*  收到客户端的请求,并解析请求投入到逻辑消息队列    */
int ClientSession::onMsg(const char* buffer, int len)
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
                const static std::vector<string> notDataRequest = { "PING\r\n" };

                bool isWaitDataCompleteRequest = false;
                bool isFindCompleteRequest = false;

                for (auto& v : notDataRequest)
                {
                    const size_t leftLen = len - totalLen;
                    if (leftLen < v.size())
                    {
                        size_t findPos = v.find(parseStartPos, 0, leftLen);
                        if (findPos != string::npos)
                        {
                            isWaitDataCompleteRequest = true;
                            break;
                        }
                    }
                    else
                    {
                        if (v.compare(0, v.size(), parseStartPos, v.size()) == 0)
                        {
                            processRequest(true, nullptr, nullptr, mCache, parseStartPos, v.size());
                            totalLen += v.size();
                            parseStartPos += v.size();

                            isFindCompleteRequest = true;
                            break;
                        }
                    }
                }
                
                if (isWaitDataCompleteRequest) /*需要等待非完整的命令，退出本次处理*/
                {
                    break;
                }

                if (isFindCompleteRequest)  /*找到完整的非数据相关命令，回到开始尝试处理下一个命令*/
                {
                    continue;;
                }

                mRedisParse = parse_tree_new(); /*构造处理数据相关redis命令解析器*/
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
            SSDBProtocolResponse* ssdbQuery = new SSDBProtocolResponse;
            ssdbQuery->parse(parseStartPos);
            processRequest(false, ssdbQuery, nullptr, mCache, parseStartPos, packetLen);

            totalLen += packetLen;
            leftLen -= packetLen;
            parseStartPos += packetLen;
        }
    }

    return totalLen;
}

/*
    isRedis：是否redis request
    ssdbQuery：解析好的ssdb request，可能为null
    redisRequest：解析好的redis request，可能为null
    requestBinary：原始的request报文(智能指针)，可能为null
    requestBuffer：原始的报文内存起始点，不可能为null
    requestLen：原始的报文长度，非0

    同时传递requestBinary和requestBuffer，是为了处理requestBinary为null的情况，如果它不为null，那么后续处理里可能直接用它作为send参数的(就可以避免再为send构造一次packet内存!!!)。
*/
void ClientSession::processRequest(bool isRedis, SSDBProtocolResponse* ssdbQuery, parse_tree* redisRequest, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen)
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
                        isSuccess = procSSDBMultiSet(ssdbQuery, requestBinary, requestBuffer, requestLen);
                    }
                    else if (strncmp("multi_get", op->buffer, op->len) == 0)
                    {
                        isSuccess = procSSDBCommandOfMultiKeys(std::make_shared<SSDBMultiGetWaitReply>(shared_from_this()), ssdbQuery, requestBinary, requestBuffer, requestLen, "multi_get");
                    }
                    else if (strncmp("multi_del", op->buffer, op->len) == 0)
                    {
                        isSuccess = procSSDBCommandOfMultiKeys(std::make_shared<SSDBMultiDelWaitReply>(shared_from_this()), ssdbQuery, requestBinary, requestBuffer, requestLen, "multi_del");
                    }
                    else
                    {
                        isSuccess = procSSDBSingleCommand(ssdbQuery, requestBinary, requestBuffer, requestLen);
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
                isSuccess = processRedisCommandOfMultiKeys(std::make_shared<RedisMgetWaitReply>(shared_from_this()), redisRequest, requestBinary, requestBuffer, requestLen, "mget");
            }
            else if (strncmp(op, "mset", oplen) == 0)
            {
                isSuccess = processRedisMset(redisRequest, requestBinary, requestBuffer, requestLen);
            }
            else if (strncmp(op, "del", oplen) == 0)
            {
                isSuccess = processRedisCommandOfMultiKeys(std::make_shared<RedisDelWaitReply>(shared_from_this()), redisRequest, requestBinary, requestBuffer, requestLen, "del");
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
}

void ClientSession::pushSSDBStrListReply(const std::vector< const char*> &strlist)
{
    std::shared_ptr<StrListSSDBReply> reply = std::make_shared<StrListSSDBReply>(shared_from_this());
    for (auto& v : strlist)
    {
        reply->pushStr(v);
    }
    mPendingReply->push_back(reply);
    processCompletedReply();
}

void ClientSession::pushSSDBErrorReply(const char* error)
{
    pushSSDBStrListReply({ "error", error });
}

void ClientSession::pushRedisErrorReply(const char* error)
{
    BaseWaitReply::PTR w = std::make_shared<RedisErrorReply>(shared_from_this(), error);
    mPendingReply->push_back(w);
    processCompletedReply();
}

void ClientSession::pushRedisStatusReply(const char* status)
{
    BaseWaitReply::PTR w = std::make_shared<RedisStatusReply>(shared_from_this(), status);
    mPendingReply->push_back(w);
    processCompletedReply();
}

bool ClientSession::procSSDBAuth(SSDBProtocolResponse* request, const char* requestBuffer, size_t requestLen)
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

bool ClientSession::procSSDBPing(SSDBProtocolResponse*, const char* requestBuffer, size_t requestLen)
{
    pushSSDBStrListReply({ "ok" });
    return true;
}

bool ClientSession::procSSDBMultiSet(SSDBProtocolResponse* request, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen)
{
    bool isSuccess = (request->getBuffersLen() - 1) % 2 == 0 && request->getBuffersLen() > 1;

    BaseWaitReply::PTR w = std::make_shared<SSDBMultiSetWaitReply>(shared_from_this());
    unordered_map<int, std::vector<Bytes>> kvsMap;

    if (isSuccess)
    {
        for (size_t i = 1; i < request->getBuffersLen(); i += 2)
        {
            Bytes* b = request->getByIndex(i);
            int serverID;
            if (sharding_key(mLua, b->buffer, b->len, serverID))
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
            auto server = findBackendByID((*kvsMap.begin()).first);
            if (server != nullptr)
            {
                server->forward(w, requestBinary, requestBuffer, requestLen);
            }
        }
        else
        {
            SSDBProtocolRequest& request2Backend = getCacheSSDBProtocol();

            for (auto& v : kvsMap)
            {
                request2Backend.init();
                auto server = findBackendByID(v.first);

                request2Backend.appendStr("multi_set");
                for (auto& k : v.second)
                {
                    request2Backend.appendStr(k.buffer, k.len);
                }
                request2Backend.endl();

                if (server != nullptr)
                {
                    server->forward(w, nullptr, request2Backend.getResult(), request2Backend.getResultLen());
                }
            }
        }

        mPendingReply->push_back(w);
    }

    return isSuccess;
}

bool ClientSession::procSSDBCommandOfMultiKeys(std::shared_ptr<BaseWaitReply> w, SSDBProtocolResponse* request, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen, const char* command)
{
    bool isSuccess = request->getBuffersLen() > 1;

    unordered_map<int, std::vector<Bytes>> serverKs;

    for (size_t i = 1; i < request->getBuffersLen(); ++i)
    {
        Bytes* b = request->getByIndex(i);
        int serverID;
        if (sharding_key(mLua, b->buffer, b->len, serverID))
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
            auto server = findBackendByID((*serverKs.begin()).first);
            if (server != nullptr)
            {
                server->forward(w, requestBinary, requestBuffer, requestLen);
            }
        }
        else
        {
            SSDBProtocolRequest& request2Backend = getCacheSSDBProtocol();

            for (auto& v : serverKs)
            {
                request2Backend.init();
                auto server = findBackendByID(v.first);

                request2Backend.appendStr(command);
                for (auto& k : v.second)
                {
                    request2Backend.appendStr(k.buffer, k.len);
                }
                request2Backend.endl();

                if (server != nullptr)
                {
                    server->forward(w, nullptr, request2Backend.getResult(), request2Backend.getResultLen());
                }
            }
        }
        mPendingReply->push_back(w);
    }

    return isSuccess;
}

bool ClientSession::procSSDBSingleCommand(SSDBProtocolResponse* request, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen)
{
    bool isSuccess = false;
    
    Bytes* b = request->getByIndex(1);
    int serverID;
    if (sharding_key(mLua, b->buffer, b->len, serverID))
    {
        isSuccess = true;
        BaseWaitReply::PTR w = std::make_shared<SSDBSingleWaitReply>(shared_from_this());
        auto server = findBackendByID(serverID);
        if (server != nullptr)
        {
            server->forward(w, requestBinary, requestBuffer, requestLen);
        }
        mPendingReply->push_back(w);
    }

    return isSuccess;
}

bool ClientSession::processRedisSingleCommand(parse_tree* parse, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen)
{
    bool isSuccess = false;

    int serverID;
    if (sharding_key(mLua, parse->reply->element[1]->str, parse->reply->element[1]->len, serverID))
    {
        auto server = findBackendByID(serverID);
        if (server != nullptr)
        {
            isSuccess = true;
            BaseWaitReply::PTR w = std::make_shared<RedisSingleWaitReply>(shared_from_this());
            server->forward(w, requestBinary, requestBuffer, requestLen);
            mPendingReply->push_back(w);
        }
    }

    return isSuccess;
}

bool ClientSession::processRedisMset(parse_tree* parse, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen)
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

        if (sharding_key(mLua, key, keyLen, serverID))
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
        BaseWaitReply::PTR w = std::make_shared<RedisMsetWaitReply>(shared_from_this());
        if (serverKvs.size() == 1)
        {
            auto server = findBackendByID((*serverKvs.begin()).first);
            if (server != nullptr)
            {
                server->forward(w, requestBinary, requestBuffer, requestLen);
            }
        }
        else
        {
            RedisProtocolRequest& request2Backend = getCacheRedisProtocol();

            for (auto& v : serverKvs)
            {
                request2Backend.init();
                request2Backend.writev("mset");
                for (auto& k : v.second)
                {
                    request2Backend.appendBinary(k.buffer, k.len);
                }
                request2Backend.endl();

                auto server = findBackendByID(v.first);
                if (server != nullptr)
                {
                    server->forward(w, nullptr, request2Backend.getResult(), request2Backend.getResultLen());
                }
            }
        }
        mPendingReply->push_back(w);
    }

    return isSuccess;
}

bool ClientSession::processRedisCommandOfMultiKeys(std::shared_ptr<BaseWaitReply> w, parse_tree* parse, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen, const char* command)
{
    bool isSuccess = parse->reply->elements > 1;

    unordered_map<int, std::vector<Bytes>> serverKs;

    for (size_t i = 1; i < parse->reply->elements; ++i)
    {
        int serverID;
        const char* key = parse->reply->element[i]->str;
        int keyLen = parse->reply->element[i]->len;

        if (sharding_key(mLua, key, keyLen, serverID))
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
            auto server = findBackendByID((*serverKs.begin()).first);
            if (server != nullptr)
            {
                server->forward(w, requestBinary, requestBuffer, requestLen);
            }
        }
        else
        {
            RedisProtocolRequest& request2Backend = getCacheRedisProtocol();

            for (auto& v : serverKs)
            {
                request2Backend.init();
                request2Backend.appendBinary(command, strlen(command));
                for (auto& k : v.second)
                {
                    request2Backend.appendBinary(k.buffer, k.len);
                }
                request2Backend.endl();

                auto server = findBackendByID(v.first);
                if (server != nullptr)
                {
                    server->forward(w, nullptr, request2Backend.getResult(), request2Backend.getResultLen());
                }
            }
        }
        mPendingReply->push_back(w);
    }

    return isSuccess;
}

void ClientSession::processCompletedReply()
{
    auto tmp = shared_from_this();
    while (!mPendingReply->empty())
    {
        auto& w = mPendingReply->front();
        bool r = false;
        w->lockWaitList();
        r = w->isAllCompleted() || w->hasError();
        w->unLockWaitList();
        if (r)
        {
            w->lockWaitList();
            w->mergeAndSend(tmp);
            w->unLockWaitList();
            mPendingReply->pop_front();
        }
        else
        {
            break;
        }
    }
}
