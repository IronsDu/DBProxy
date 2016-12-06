#include <iostream>

#include "protocol/RedisParse.h"
#include "protocol/RedisRequest.h"
#include "Backend.h"
#include "protocol/SSDBProtocol.h"
#include "SSDBWaitReply.h"
#include "RedisWaitReply.h"

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

using namespace std;

ClientSession::ClientSession()
{
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

void ClientSession::onEnter()
{
}

void ClientSession::onClose()
{
}

/*  收到客户端的请求,并解析请求投入到逻辑消息队列    */
size_t ClientSession::onMsg(const char* buffer, size_t len)
{
    size_t totalLen = 0;

    if (mRedisParse != nullptr ||
        !IS_NUM(buffer[0]))
    {
        totalLen = onRedisRequestMsg(buffer, len);
    }
    else
    {
        totalLen = onSSDBRequestMsg(buffer, len);
    }

    return totalLen;
}

size_t ClientSession::onRedisRequestMsg(const char* buffer, size_t len)
{
    size_t totalLen = 0;

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
                        processRedisRequest(mCache, parseStartPos, v.size());
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

        int parseRet = parse(mRedisParse, &parseEndPos, (char*)buffer + len);
        totalLen += (parseEndPos - parseStartPos);

        if (parseRet == REDIS_OK)
        {
            if (mCache == nullptr)
            {
                processRedisRequest(mCache, parseStartPos, parseEndPos - parseStartPos);
            }
            else
            {
                mCache->append(parseStartPos, parseEndPos - parseStartPos);
                processRedisRequest(mCache, mCache->c_str(), mCache->size());
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

    return totalLen;
}

size_t ClientSession::onSSDBRequestMsg(const char* buffer, size_t len)
{
    size_t totalLen = 0;

    char* parseStartPos = (char*)buffer;
    int leftLen = len;
    int packetLen = 0;

    while ((packetLen = SSDBProtocolResponse::check_ssdb_packet(parseStartPos, leftLen)) > 0)
    {
        /*  TODO::SSDBProtocolResponse使用智能指针    */
        SSDBProtocolResponse* ssdbQuery = new SSDBProtocolResponse;
        ssdbQuery->parse(parseStartPos);

        processSSDBRequest(ssdbQuery, mCache, parseStartPos, packetLen);

        delete ssdbQuery;
        ssdbQuery = nullptr;

        totalLen += packetLen;
        leftLen -= packetLen;
        parseStartPos += packetLen;
    }

    return totalLen;
}

void ClientSession::processRedisRequest(std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen)
{
    /*TODO::重用网络层分包的命令判断，减少此处的字符串比较*/
    if (strncmp(requestBuffer, "PING\r\n", 6) == 0)
    {
        pushRedisStatusReply("PONG");
    }
    else
    {
        const char* op = mRedisParse->reply->element[0]->str;
        const size_t oplen = mRedisParse->reply->element[0]->len;

        bool isSuccess = false;

        if (strncmp(op, "PING", 4) == 0)
        {
            pushRedisStatusReply("PONG");
        }
        else if (strncmp(op, "mget", oplen) == 0)
        {
            isSuccess = processRedisCommandOfMultiKeys(std::make_shared<RedisMgetWaitReply>(shared_from_this()), mRedisParse, requestBinary, requestBuffer, requestLen, "mget");
        }
        else if (strncmp(op, "mset", oplen) == 0)
        {
            isSuccess = processRedisMset(mRedisParse, requestBinary, requestBuffer, requestLen);
        }
        else if (strncmp(op, "del", oplen) == 0)
        {
            isSuccess = processRedisCommandOfMultiKeys(std::make_shared<RedisDelWaitReply>(shared_from_this()), mRedisParse, requestBinary, requestBuffer, requestLen, "del");
        }
        else
        {
            isSuccess = processRedisSingleCommand(mRedisParse, requestBinary, requestBuffer, requestLen);
        }

        if (!isSuccess)
        {
            /*  模拟一个错误  */
            pushRedisErrorReply("no error for key");
        }
    }

    if (mRedisParse != nullptr)
    {
        parse_tree_del(mRedisParse);
        mRedisParse = nullptr;
    }
}

void ClientSession::processSSDBRequest(SSDBProtocolResponse* ssdbQuery, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen)
{
    bool isSuccess = false;

    if (!gBackendClients.empty())
    {
        Bytes* op = ssdbQuery->getByIndex(0);
        if (op != nullptr)
        {
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

void ClientSession::pushSSDBStrListReply(const std::vector< const char*> &strlist)
{
    auto reply = std::make_shared<StrListSSDBReply>(shared_from_this());
    for (auto& v : strlist)
    {
        reply->pushStr(v);
    }
    mPendingReply.push_back(reply);
    processCompletedReply();
}

void ClientSession::pushSSDBErrorReply(const char* error)
{
    pushSSDBStrListReply({ "error", error });
}

void ClientSession::pushRedisErrorReply(const char* error)
{
    auto waitReply = std::make_shared<RedisErrorReply>(shared_from_this(), error);
    mPendingReply.push_back(waitReply);
    processCompletedReply();
}

void ClientSession::pushRedisStatusReply(const char* status)
{
    auto waitReply = std::make_shared<RedisStatusReply>(shared_from_this(), status);
    mPendingReply.push_back(waitReply);
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

    BaseWaitReply::PTR waitReply = std::make_shared<SSDBMultiSetWaitReply>(shared_from_this());

    if (isSuccess)
    {
        for (size_t i = 1; i < request->getBuffersLen(); i += 2)
        {
            Bytes* b = request->getByIndex(i);
            int serverID;
            if (sharding_key(mLua, b->buffer, b->len, serverID))
            {
                auto it = mShardingTmpKVS.find(serverID);
                if (it == mShardingTmpKVS.end())
                {
                    std::vector<Bytes> tmp;
                    tmp.push_back(*b);
                    tmp.push_back(*(request->getByIndex(i + 1)));
                    mShardingTmpKVS[serverID] = std::move(tmp);
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
        if (mShardingTmpKVS.size() == 1)
        {
            auto server = findBackendByID((*mShardingTmpKVS.begin()).first);
            if (server != nullptr)
            {
                server->forward(waitReply, requestBinary, requestBuffer, requestLen);
            }
        }
        else
        {
            SSDBProtocolRequest& request2Backend = getCacheSSDBProtocol();

            for (auto& v : mShardingTmpKVS)
            {
                if (!v.second.empty())
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
                        server->forward(waitReply, nullptr, request2Backend.getResult(), request2Backend.getResultLen());
                    }
                }
            }
        }

        mPendingReply.push_back(waitReply);
    }

    clearShardingKVS();

    return isSuccess;
}

bool ClientSession::procSSDBCommandOfMultiKeys(std::shared_ptr<BaseWaitReply> waitReply, SSDBProtocolResponse* request, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen, const char* command)
{
    bool isSuccess = request->getBuffersLen() > 1;

    for (size_t i = 1; i < request->getBuffersLen(); ++i)
    {
        Bytes* b = request->getByIndex(i);
        int serverID;
        if (sharding_key(mLua, b->buffer, b->len, serverID))
        {
            auto it = mShardingTmpKVS.find(serverID);
            if (it == mShardingTmpKVS.end())
            {
                std::vector<Bytes> tmp;
                tmp.push_back(*b);
                mShardingTmpKVS[serverID] = std::move(tmp);
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
        if (mShardingTmpKVS.size() == 1)
        {
            auto server = findBackendByID((*mShardingTmpKVS.begin()).first);
            if (server != nullptr)
            {
                server->forward(waitReply, requestBinary, requestBuffer, requestLen);
            }
        }
        else
        {
            SSDBProtocolRequest& request2Backend = getCacheSSDBProtocol();

            for (auto& v : mShardingTmpKVS)
            {
               if (!v.second.empty())
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
                       server->forward(waitReply, nullptr, request2Backend.getResult(), request2Backend.getResultLen());
                   }
               }
            }
        }

        mPendingReply.push_back(waitReply);
    }

    clearShardingKVS();

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
        BaseWaitReply::PTR waitReply = std::make_shared<SSDBSingleWaitReply>(shared_from_this());
        auto server = findBackendByID(serverID);
        if (server != nullptr)
        {
            server->forward(waitReply, requestBinary, requestBuffer, requestLen);
        }
        mPendingReply.push_back(waitReply);
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
            BaseWaitReply::PTR waitReply = std::make_shared<RedisSingleWaitReply>(shared_from_this());
            server->forward(waitReply, requestBinary, requestBuffer, requestLen);
            mPendingReply.push_back(waitReply);
        }
    }

    return isSuccess;
}

bool ClientSession::processRedisMset(parse_tree* parse, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen)
{
    bool isSuccess = parse->reply->elements > 1 && (parse->reply->elements-1) % 2 == 0;

    for (size_t i = 1; i < parse->reply->elements; i+=2)
    {
        int serverID;

        const char* key = parse->reply->element[i]->str;
        int keyLen = parse->reply->element[i]->len;
        const char* value = parse->reply->element[i+1]->str;
        int valueLen = parse->reply->element[i+1]->len;

        if (sharding_key(mLua, key, keyLen, serverID))
        {
            auto it = mShardingTmpKVS.find(serverID);
            if (it == mShardingTmpKVS.end())
            {
                std::vector<Bytes> tmp;
                tmp.push_back({key, keyLen});
                tmp.push_back({value,valueLen});
                mShardingTmpKVS[serverID] = std::move(tmp);
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
        BaseWaitReply::PTR waitReply = std::make_shared<RedisMsetWaitReply>(shared_from_this());
        if (mShardingTmpKVS.size() == 1)
        {
            auto server = findBackendByID((*mShardingTmpKVS.begin()).first);
            if (server != nullptr)
            {
                server->forward(waitReply, requestBinary, requestBuffer, requestLen);
            }
        }
        else
        {
            RedisProtocolRequest& request2Backend = getCacheRedisProtocol();

            for (auto& v : mShardingTmpKVS)
            {
                if (!v.second.empty())
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
                        server->forward(waitReply, nullptr, request2Backend.getResult(), request2Backend.getResultLen());
                    }
                }
            }
        }

        mPendingReply.push_back(waitReply);
    }

    clearShardingKVS();

    return isSuccess;
}

bool ClientSession::processRedisCommandOfMultiKeys(std::shared_ptr<BaseWaitReply> waitReply, parse_tree* parse, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen, const char* command)
{
    bool isSuccess = parse->reply->elements > 1;

    for (size_t i = 1; i < parse->reply->elements; ++i)
    {
        int serverID;
        const char* key = parse->reply->element[i]->str;
        int keyLen = parse->reply->element[i]->len;

        if (sharding_key(mLua, key, keyLen, serverID))
        {
            auto it = mShardingTmpKVS.find(serverID);
            if (it == mShardingTmpKVS.end())
            {
                std::vector<Bytes> tmp;
                tmp.push_back({key, keyLen});
                mShardingTmpKVS[serverID] = std::move(tmp);
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
        if (mShardingTmpKVS.size() == 1)
        {
            auto server = findBackendByID((*mShardingTmpKVS.begin()).first);
            if (server != nullptr)
            {
                server->forward(waitReply, requestBinary, requestBuffer, requestLen);
            }
        }
        else
        {
            RedisProtocolRequest& request2Backend = getCacheRedisProtocol();

            for (auto& v : mShardingTmpKVS)
            {
                if (!v.second.empty())
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
                        server->forward(waitReply, nullptr, request2Backend.getResult(), request2Backend.getResultLen());
                    }
                }
            }
        }

        mPendingReply.push_back(waitReply);
    }

    clearShardingKVS();

    return isSuccess;
}

void ClientSession::clearShardingKVS()
{
    for (auto& v : mShardingTmpKVS)
    {
        v.second.clear();
    }
}

void ClientSession::processCompletedReply()
{
    auto sharedThis = shared_from_this();
    while (!mPendingReply.empty())
    {
        auto& waitReply = mPendingReply.front();
        if (waitReply->isAllCompleted() || waitReply->hasError())
        {
            waitReply->lockReply();
            waitReply->mergeAndSend(sharedThis);
            waitReply->unLockReply();
            mPendingReply.pop_front();
        }
        else
        {
            break;
        }
    }
}
