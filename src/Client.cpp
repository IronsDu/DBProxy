#include <iostream>

#include "protocol/RedisParse.h"
#include "protocol/RedisRequest.h"
#include "Backend.h"
#include "protocol/SSDBProtocol.h"
#include "SSDBWaitReply.h"
#include "RedisWaitReply.h"

#include "defer.h"
#include "Client.h"

using namespace std;

ClientSession::ClientSession(brynet::net::TCPSession::PTR session,
    sol::state state,
    std::string shardingFunction)
    :
    BaseSession(session),
    mLuaState(std::move(state)),
    mShardingFunction(std::move(shardingFunction))
{
    cout << "ClientSession::ClientSession()" << endl;
    mRedisParse = nullptr;
    mNeedAuth = false;
    mIsAuth = false;
}

ClientSession::~ClientSession()
{
    cout << "ClientSession::~ClientSession()" << endl;
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
            const static std::vector<string> notDataRequest = { "PING\r\n", "COMMAND\r\n" };

            bool isWaitDataCompleteRequest = false;
            bool isFindCompleteRequest = false;

            for (const auto& v : notDataRequest)
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

            mRedisParse = std::shared_ptr<parse_tree>(parse_tree_new(), [](parse_tree* parse) {
                parse_tree_del(parse);
            }); /*构造处理数据相关redis命令解析器*/
        }

        int parseRet = parse(mRedisParse.get(), &parseEndPos, (char*)buffer + len);
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
                auto tmp = mCache->c_str();
                auto tmpLen = mCache->size();
                processRedisRequest(mCache, tmp, mCache->size());
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
        auto ssdbQuery = std::make_shared<SSDBProtocolResponse>();
        ssdbQuery->parse(parseStartPos);

        processSSDBRequest(ssdbQuery, mCache, parseStartPos, packetLen);

        totalLen += packetLen;
        leftLen -= packetLen;
        parseStartPos += packetLen;
    }

    return totalLen;
}

void ClientSession::processRedisRequest(const std::shared_ptr<std::string>& requestBinary, 
    const char* requestBuffer, 
    size_t requestLen)
{
    defer (mRedisParse = nullptr);

    if (strncmp(requestBuffer, "PING\r\n", 6) == 0)
    {
        pushRedisStatusReply("PONG");
        return;
    }

    const char* op = mRedisParse->reply->element[0]->str;
    const size_t oplen = mRedisParse->reply->element[0]->len;

    bool isSuccess = false;
    defer (
        if (!isSuccess)
        {
            /*  模拟一个错误  */
            pushRedisErrorReply("no error for key");
        }
    );

    if (strncmp(op, "ping", 4) == 0 || strncmp(op, "PING", 4) == 0)
    {
        pushRedisStatusReply("PONG");
        isSuccess = true;
    }
    else if (strncmp(op, "COMMAND", 7) == 0 || strncmp(op, "command", 7) == 0)
    {
        std::shared_ptr<std::string> tmp = std::make_shared<string>("*6\r\n");
        tmp->append("$4\r\n");
        tmp->append("haha\r\n");
        tmp->append(":1\r\n");

        tmp->append("*1\r\n");
        tmp->append(":1\r\n");

        tmp->append(":1\r\n");
        tmp->append(":1\r\n");
        tmp->append(":1\r\n");
        send(tmp);
        return;
    }
    else if (strncmp(op, "mget", oplen) == 0 ||
        strncmp(op, "del", oplen) == 0)
    {
        isSuccess = processRedisCommandOfMultiKeys(std::make_shared<RedisMgetWaitReply>(shared_from_this()),
            mRedisParse,
            requestBinary,
            requestBuffer,
            requestLen,
            op);
    }
    else if (strncmp(op, "mset", oplen) == 0)
    {
        isSuccess = processRedisMset(mRedisParse, requestBinary, requestBuffer, requestLen);
    }
    else
    {
        isSuccess = processRedisSingleCommand(mRedisParse, requestBinary, requestBuffer, requestLen);
    }
}

void ClientSession::processSSDBRequest(const std::shared_ptr<SSDBProtocolResponse>& ssdbQuery, 
    const std::shared_ptr<std::string>& requestBinary, 
    const char* requestBuffer, 
    size_t requestLen)
{
    bool isSuccess = false;

    defer (
        if (!isSuccess)
        {
            /*  模拟一个错误  */
            pushSSDBErrorReply("command not process");
        }
    );

    Bytes* op = ssdbQuery->getByIndex(0);
    if (op == nullptr)
    {
        return;
    }

    if (strncmp("auth", op->buffer, op->len) == 0)
    {
        isSuccess = procSSDBAuth(ssdbQuery, requestBuffer, requestLen);
        return;
    }
    
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
        isSuccess = procSSDBCommandOfMultiKeys(std::make_shared<SSDBMultiGetWaitReply>(shared_from_this()),
            ssdbQuery,
            requestBinary,
            requestBuffer,
            requestLen,
            "multi_get");
    }
    else if (strncmp("multi_del", op->buffer, op->len) == 0)
    {
        isSuccess = procSSDBCommandOfMultiKeys(std::make_shared<SSDBMultiDelWaitReply>(shared_from_this()),
            ssdbQuery,
            requestBinary,
            requestBuffer,
            requestLen,
            "multi_del");
    }
    else
    {
        isSuccess = procSSDBSingleCommand(ssdbQuery, requestBinary, requestBuffer, requestLen);
    }
}

void ClientSession::pushSSDBStrListReply(const std::vector< const char*> &strlist)
{
    auto reply = std::make_shared<StrListSSDBReply>(shared_from_this());
    for (const auto& v : strlist)
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
    mPendingReply.push_back(std::make_shared<RedisErrorReply>(shared_from_this(), error));
    processCompletedReply();
}

void ClientSession::pushRedisStatusReply(const char* status)
{
    mPendingReply.push_back(std::make_shared<RedisStatusReply>(shared_from_this(), status));
    processCompletedReply();
}

bool ClientSession::procSSDBAuth(const std::shared_ptr<SSDBProtocolResponse>& request, 
    const char* requestBuffer, 
    size_t requestLen)
{
    if (request->getBuffersLen() != 2)
    {
        pushSSDBStrListReply({ "client_error" });
        return true;
    }

    Bytes* p = request->getByIndex(1);
    if (mNeedAuth && strncmp(p->buffer, mPassword.c_str(), p->len) != 0)
    {
        pushSSDBErrorReply("invalid password");
        return true;
    }

    mIsAuth = true;
    pushSSDBStrListReply({ "ok" });

    return true;
}

bool ClientSession::procSSDBPing(const std::shared_ptr<SSDBProtocolResponse>&, 
    const char* requestBuffer, 
    size_t requestLen)
{
    pushSSDBStrListReply({ "ok" });
    return true;
}

bool ClientSession::procSSDBMultiSet(const std::shared_ptr<SSDBProtocolResponse>& request, 
    const std::shared_ptr<std::string>& requestBinary, 
    const char* requestBuffer, 
    size_t requestLen)
{
    bool isSuccess = (request->getBuffersLen() - 1) % 2 == 0 && request->getBuffersLen() > 1;
    if (!isSuccess)
    {
        return isSuccess;
    }
    
    BaseWaitReply::PTR waitReply = std::make_shared<SSDBMultiSetWaitReply>(shared_from_this());

    defer (
        clearShardingKVS();
        if (isSuccess)
        {
            mPendingReply.push_back(waitReply);
        }
    );

    for (size_t i = 1; i < request->getBuffersLen(); i += 2)
    {
        const Bytes* b = request->getByIndex(i);
        int serverID;
        if (!shardingKey(b->buffer, b->len, serverID))
        {
            return isSuccess = false;
        }

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

    if (mShardingTmpKVS.size() == 1)
    {
        auto server = findBackendByID((*mShardingTmpKVS.begin()).first);
        if (server == nullptr)
        {
            return isSuccess = false;
        }

        server->forward(waitReply, requestBinary, requestBuffer, requestLen);
    }
    else
    {
        SSDBProtocolRequest& request2Backend = getCacheSSDBProtocol();

        for (auto& v : mShardingTmpKVS)
        {
            if (v.second.empty())
            {
                continue;
            }

            request2Backend.init();
            auto server = findBackendByID(v.first);
            if (server == nullptr)
            {
                return isSuccess = false;
            }

            request2Backend.appendStr("multi_set");
            for (auto& k : v.second)
            {
                request2Backend.appendStr(k.buffer, k.len);
            }
            request2Backend.endl();

            server->forward(waitReply,
                nullptr,
                request2Backend.getResult(),
                request2Backend.getResultLen());
        }
    }

    return isSuccess;
}

bool ClientSession::procSSDBCommandOfMultiKeys(const std::shared_ptr<BaseWaitReply>& waitReply, 
    const std::shared_ptr<SSDBProtocolResponse>& request, 
    const std::shared_ptr<std::string>& requestBinary, 
    const char* requestBuffer, 
    size_t requestLen, 
    const char* command)
{
    bool isSuccess = request->getBuffersLen() > 1;
    if (!isSuccess)
    {
        return false;
    }

    defer (
        clearShardingKVS();
        if (isSuccess)
        {
            mPendingReply.push_back(waitReply);
        }
    );

    for (size_t i = 1; i < request->getBuffersLen(); ++i)
    {
        Bytes* b = request->getByIndex(i);
        int serverID;
        if (!shardingKey(b->buffer, b->len, serverID))
        {
            return isSuccess = false;
        }

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

    if (mShardingTmpKVS.size() == 1)
    {
        auto server = findBackendByID((*mShardingTmpKVS.begin()).first);
        if (server == nullptr)
        {
            return isSuccess = false;
        }

        server->forward(waitReply, requestBinary, requestBuffer, requestLen);
    }
    else
    {
        SSDBProtocolRequest& request2Backend = getCacheSSDBProtocol();

        for (auto& v : mShardingTmpKVS)
        {
            if (v.second.empty())
            {
                continue;
            }

            request2Backend.init();
            auto server = findBackendByID(v.first);
            if (server == nullptr)
            {
                return isSuccess = false;
            }

            request2Backend.appendStr(command);
            for (auto& k : v.second)
            {
                request2Backend.appendStr(k.buffer, k.len);
            }
            request2Backend.endl();

            server->forward(waitReply, nullptr, request2Backend.getResult(), request2Backend.getResultLen());
        }
    }

    return true;
}

bool ClientSession::procSSDBSingleCommand(const std::shared_ptr<SSDBProtocolResponse>& request, 
    const std::shared_ptr<std::string>& requestBinary, 
    const char* requestBuffer, size_t requestLen)
{
    Bytes* b = request->getByIndex(1);
    int serverID;
    if (!shardingKey(b->buffer, b->len, serverID))
    {
        return false;
    }

    auto server = findBackendByID(serverID);
    if (server == nullptr)
    {
        return false;
    }

    BaseWaitReply::PTR waitReply = std::make_shared<SSDBSingleWaitReply>(shared_from_this());
    server->forward(waitReply, requestBinary, requestBuffer, requestLen);
    mPendingReply.push_back(waitReply);
    
    return true;
}

bool ClientSession::processRedisSingleCommand(const std::shared_ptr<parse_tree>& parse, 
    const std::shared_ptr<std::string>& requestBinary, 
    const char* requestBuffer, 
    size_t requestLen)
{
    if (parse->reply->elements < 1)
    {
        return false;
    }

    int serverID;
    if (!shardingKey(parse->reply->element[1]->str, parse->reply->element[1]->len, serverID))
    {
        return false;
    }

    auto server = findBackendByID(serverID);
    if (server == nullptr)
    {
        return false;
    }

    BaseWaitReply::PTR waitReply = std::make_shared<RedisSingleWaitReply>(shared_from_this());
    server->forward(waitReply, requestBinary, requestBuffer, requestLen);
    mPendingReply.push_back(waitReply);

    return true;
}

bool ClientSession::processRedisMset(const std::shared_ptr<parse_tree>& parse, 
    const std::shared_ptr<std::string>& requestBinary, 
    const char* requestBuffer, 
    size_t requestLen)
{
    if (parse->reply->elements <= 1 || (parse->reply->elements - 1) % 2 != 0)
    {
        return false;
    }

    defer (
        clearShardingKVS();
    );

    for (size_t i = 1; i < parse->reply->elements; i+=2)
    {
        int serverID;

        const char* key = parse->reply->element[i]->str;
        int keyLen = parse->reply->element[i]->len;
        const char* value = parse->reply->element[i+1]->str;
        int valueLen = parse->reply->element[i+1]->len;

        if (!shardingKey(key, keyLen, serverID))
        {
            return false;
        }

        auto it = mShardingTmpKVS.find(serverID);
        if (it == mShardingTmpKVS.end())
        {
            std::vector<Bytes> tmp;
            tmp.push_back({ key, keyLen });
            tmp.push_back({ value, valueLen });
            mShardingTmpKVS[serverID] = std::move(tmp);
        }
        else
        {
            (*it).second.push_back({ key, keyLen });
            (*it).second.push_back({ value, valueLen });
        }
    }

    BaseWaitReply::PTR waitReply = std::make_shared<RedisMsetWaitReply>(shared_from_this());
    auto isSuccess = true;

    defer (
        if (isSuccess)
        {
            mPendingReply.push_back(waitReply);
        }
    );

    if (mShardingTmpKVS.size() == 1)
    {
        auto server = findBackendByID((*mShardingTmpKVS.begin()).first);
        if (server == nullptr)
        {
            return isSuccess = false;
        }

        server->forward(waitReply, requestBinary, requestBuffer, requestLen);
    }
    else
    {
        RedisProtocolRequest& request2Backend = getCacheRedisProtocol();

        for (auto& v : mShardingTmpKVS)
        {
            if (v.second.empty())
            {
                continue;
            }

            auto server = findBackendByID(v.first);
            if (server == nullptr)
            {
                return isSuccess = false;
            }

            request2Backend.init();
            request2Backend.writev("mset");
            for (auto& k : v.second)
            {
                request2Backend.appendBinary(k.buffer, k.len);
            }
            request2Backend.endl();


            server->forward(waitReply, nullptr, request2Backend.getResult(), request2Backend.getResultLen());
        }
    }

    return true;
}

bool ClientSession::processRedisCommandOfMultiKeys(const std::shared_ptr<BaseWaitReply>& waitReply, 
    const std::shared_ptr<parse_tree>& parse, 
    const std::shared_ptr<std::string>& requestBinary, 
    const char* requestBuffer, 
    size_t requestLen, 
    const char* command)
{
    if (parse->reply->elements <= 1)
    {
        return false;
    }

    defer (
        clearShardingKVS();
    );

    for (size_t i = 1; i < parse->reply->elements; ++i)
    {
        int serverID;
        const char* key = parse->reply->element[i]->str;
        int keyLen = parse->reply->element[i]->len;

        if (!shardingKey(key, keyLen, serverID))
        {
            return false;
        }

        auto it = mShardingTmpKVS.find(serverID);
        if (it == mShardingTmpKVS.end())
        {
            std::vector<Bytes> tmp;
            tmp.push_back({ key, keyLen });
            mShardingTmpKVS[serverID] = std::move(tmp);
        }
        else
        {
            (*it).second.push_back({ key, keyLen });
        }
    }

    auto isSuccess = true;
    defer (
        if (isSuccess)
        {
            mPendingReply.push_back(waitReply);
        };
    );

    if (mShardingTmpKVS.size() == 1)
    {
        auto server = findBackendByID((*mShardingTmpKVS.begin()).first);
        if (server == nullptr)
        {
            return isSuccess = false;
        }

        server->forward(waitReply, requestBinary, requestBuffer, requestLen);
    }
    else
    {
        RedisProtocolRequest& request2Backend = getCacheRedisProtocol();

        for (const auto& v : mShardingTmpKVS)
        {
            if (v.second.empty())
            {
                continue;
            }

            auto server = findBackendByID(v.first);
            if (server == nullptr)
            {
                return isSuccess = false;
            }

            request2Backend.init();
            request2Backend.appendBinary(command, strlen(command));
            for (auto& k : v.second)
            {
                request2Backend.appendBinary(k.buffer, k.len);
            }
            request2Backend.endl();

            server->forward(waitReply, nullptr, request2Backend.getResult(), request2Backend.getResultLen());
        }
    }    

    return true;
}

void ClientSession::clearShardingKVS()
{
    for (auto& v : mShardingTmpKVS)
    {
        v.second.clear();
    }
}

bool ClientSession::shardingKey(const char * str, int len, int & serverID)
{
    serverID = mLuaState[mShardingFunction](std::string(str, len));
    return true;
}

void ClientSession::processCompletedReply()
{
    auto sharedThis = shared_from_this();
    while (!mPendingReply.empty())
    {
        const auto& waitReply = mPendingReply.front();
        if (!waitReply->isAllCompleted() && !waitReply->hasError())
        {
            break;
        }

        waitReply->mergeAndSend(sharedThis);
        mPendingReply.pop_front();
    }
}
