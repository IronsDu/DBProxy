#ifndef _CLIENT_H
#define _CLIENT_H

#include <brynet/net/TcpService.hpp>
#include <deque>
#include <memory>
#include <sol/sol.hpp>
#include <string>
#include <unordered_map>
#include <vector>

#include "BaseSession.h"
#include "protocol/RedisRequest.h"
#include "protocol/SSDBProtocol.h"

struct parse_tree;
class BaseWaitReply;
class SSDBProtocolResponse;

class ClientSession : public BaseSession, public std::enable_shared_from_this<ClientSession>
{
public:
    using PTR = std::shared_ptr<ClientSession>;

public:
    ClientSession(brynet::net::TcpConnection::Ptr session, sol::state state, std::string shardingFunction);
    ~ClientSession() = default;
    void processCompletedReply();

    RedisProtocolRequest& getCacheRedisProtocol();
    SSDBProtocolRequest& getCacheSSDBProtocol();

private:
    virtual size_t onMsg(const char* buffer, size_t len) override;
    void onEnter() override;
    void onClose() override;

private:
    size_t onRedisRequestMsg(const char* buffer, size_t len);
    size_t onSSDBRequestMsg(const char* buffer, size_t len);

    void processRedisRequest(
            const std::shared_ptr<std::string>& requestBinary,
            const char* requestBuffer,
            size_t requestLen);
    void processSSDBRequest(
            const std::shared_ptr<SSDBProtocolResponse>& ssdbQuery,
            const std::shared_ptr<std::string>& requestBinary,
            const char* requestBuffer,
            size_t requestLen);

private:
    void pushSSDBStrListReply(const std::vector<const char*>& strlist);
    void pushSSDBErrorReply(const char* error);
    void pushRedisErrorReply(const char* error);
    void pushRedisStatusReply(const char* status);

private:
    bool procSSDBAuth(
            const std::shared_ptr<SSDBProtocolResponse>&,
            const char* requestBuffer,
            size_t requestLen);
    bool procSSDBPing(
            const std::shared_ptr<SSDBProtocolResponse>&,
            const char* requestBuffer,
            size_t requestLen);
    bool procSSDBMultiSet(const std::shared_ptr<SSDBProtocolResponse>&,
                          const std::shared_ptr<std::string>& requestBinary,
                          const char* requestBuffer,
                          size_t requestLen);
    bool procSSDBCommandOfMultiKeys(
            const std::shared_ptr<BaseWaitReply>&,
            const std::shared_ptr<SSDBProtocolResponse>&,
            const std::shared_ptr<std::string>& requestBinary,
            const char* requestBuffer,
            size_t requestLen,
            const char* command);
    bool procSSDBSingleCommand(
            const std::shared_ptr<SSDBProtocolResponse>&,
            const std::shared_ptr<std::string>& requestBinary,
            const char* requestBuffer,
            size_t requestLen);

    bool processRedisSingleCommand(
            const std::shared_ptr<parse_tree>& parse,
            const std::shared_ptr<std::string>& requestBinary,
            const char* requestBuffer,
            size_t requestLen);
    bool processRedisMset(
            const std::shared_ptr<parse_tree>& parse,
            const std::shared_ptr<std::string>& requestBinary,
            const char* requestBuffer,
            size_t requestLen);
    bool processRedisCommandOfMultiKeys(
            const std::shared_ptr<BaseWaitReply>&,
            const std::shared_ptr<parse_tree>& parse,
            const std::shared_ptr<std::string>& requestBinary,
            const char* requestBuffer,
            size_t requestLen,
            const char* command);

private:
    void clearShardingKVS();
    bool shardingKey(const char* str, int len, int& serverID);

private:
    const sol::state mLuaState;
    const std::string mShardingFunction;

    std::shared_ptr<parse_tree> mRedisParse;
    std::shared_ptr<std::string> mCache;

    std::deque<std::shared_ptr<BaseWaitReply>> mPendingReply;
    bool mNeedAuth;
    bool mIsAuth;
    std::string mPassword;

    RedisProtocolRequest mCacheRedisProtocol;
    SSDBProtocolRequest mCacheSSDBProtocol;

    std::unordered_map<int, std::vector<Bytes>> mShardingTmpKVS;
};

#endif
