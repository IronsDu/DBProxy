#ifndef _CLIENT_H
#define _CLIENT_H

#include <memory>
#include <deque>
#include <string>
#include <vector>
#include <unordered_map>

#include "NetSession.h"

#include "protocol/SSDBProtocol.h"
#include "protocol/RedisRequest.h"

struct parse_tree;
class BaseWaitReply;
class SSDBProtocolResponse;

/*  代理服务器的客户端(网络线程)网络层会话    */
class ClientSession : public BaseNetSession, public std::enable_shared_from_this<ClientSession>
{
public:
    typedef std::shared_ptr<ClientSession> PTR;

public:
    ClientSession();
    ~ClientSession();

    void            processCompletedReply();

    RedisProtocolRequest&   getCacheRedisProtocol();
    SSDBProtocolRequest&    getCacheSSDBProtocol();

private:
    virtual size_t  onMsg(const char* buffer, size_t len) override;
    void            onEnter() override;
    void            onClose() override;

private:
    size_t          onRedisRequestMsg(const char* buffer, size_t len);
    size_t          onSSDBRequestMsg(const char* buffer, size_t len);

    void            processRedisRequest(std::shared_ptr<std::string> requestBinary, const char* requestBuffer, size_t requestLen);
    void            processSSDBRequest(const std::shared_ptr<SSDBProtocolResponse>& ssdbQuery, std::shared_ptr<std::string> requestBinary, const char* requestBuffer, size_t requestLen);

private:
    void            pushSSDBStrListReply(const std::vector < const char* > &strlist);
    void            pushSSDBErrorReply(const char* error);
    void            pushRedisErrorReply(const char* error);
    void            pushRedisStatusReply(const char* status);

private:
    bool            procSSDBAuth(const std::shared_ptr<SSDBProtocolResponse>&, const char* requestBuffer, size_t requestLen);
    bool            procSSDBPing(const std::shared_ptr<SSDBProtocolResponse>&, const char* requestBuffer, size_t requestLen);
    bool            procSSDBMultiSet(const std::shared_ptr<SSDBProtocolResponse>&, std::shared_ptr<std::string> requestBinary, const char* requestBuffer, size_t requestLen);
    bool            procSSDBCommandOfMultiKeys(std::shared_ptr<BaseWaitReply>, const std::shared_ptr<SSDBProtocolResponse>&, std::shared_ptr<std::string> requestBinary, const char* requestBuffer, size_t requestLen, const char* command);
    bool            procSSDBSingleCommand(const std::shared_ptr<SSDBProtocolResponse>&, std::shared_ptr<std::string> requestBinary, const char* requestBuffer, size_t requestLen);

    bool            processRedisSingleCommand(const std::shared_ptr<parse_tree>& parse, std::shared_ptr<std::string> requestBinary, const char* requestBuffer, size_t requestLen);
    bool            processRedisMset(const std::shared_ptr<parse_tree>& parse, std::shared_ptr<std::string> requestBinary, const char* requestBuffer, size_t requestLen);
    bool            processRedisCommandOfMultiKeys(std::shared_ptr<BaseWaitReply> w, const std::shared_ptr<parse_tree>& parse, std::shared_ptr<std::string> requestBinary, const char* requestBuffer, size_t requestLen, const char* command);

private:
    void            clearShardingKVS();

private:
    std::shared_ptr<parse_tree>                     mRedisParse;
    std::shared_ptr<std::string>                    mCache;

    std::deque<std::shared_ptr<BaseWaitReply>>      mPendingReply;
    bool                                            mNeedAuth;
    bool                                            mIsAuth;
    std::string                                     mPassword;
    struct lua_State*                               mLua;

    RedisProtocolRequest                            mCacheRedisProtocol;
    SSDBProtocolRequest                             mCacheSSDBProtocol;

    std::unordered_map<int, std::vector<Bytes>>     mShardingTmpKVS;
};

#endif