#ifndef _CLIENT_H
#define _CLIENT_H

#include <memory>
#include <deque>
#include <string>
#include <vector>
#include <unordered_map>

#include "NetSession.h"

#include "SSDBProtocol.h"
#include "RedisRequest.h"

struct parse_tree;
class BaseWaitReply;
class SSDBProtocolResponse;

/*  代理服务器的客户端(网络线程)网络层会话    */
class ClientSession : public BaseNetSession, public std::enable_shared_from_this<ClientSession>
{
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

    void            processRedisRequest(std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen);
    void            processSSDBRequest(SSDBProtocolResponse* ssdbQuery, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen);

private:
    void            pushSSDBStrListReply(const std::vector < const char* > &strlist);
    void            pushSSDBErrorReply(const char* error);
    void            pushRedisErrorReply(const char* error);
    void            pushRedisStatusReply(const char* status);

private:
    bool            procSSDBAuth(SSDBProtocolResponse*, const char* requestBuffer, size_t requestLen);
    bool            procSSDBPing(SSDBProtocolResponse*, const char* requestBuffer, size_t requestLen);
    bool            procSSDBMultiSet(SSDBProtocolResponse*, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen);
    bool            procSSDBCommandOfMultiKeys(std::shared_ptr<BaseWaitReply>, SSDBProtocolResponse*, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen, const char* command);
    bool            procSSDBSingleCommand(SSDBProtocolResponse*, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen);

    bool            processRedisSingleCommand(parse_tree* parse, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen);
    bool            processRedisMset(parse_tree* parse, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen);
    bool            processRedisCommandOfMultiKeys(std::shared_ptr<BaseWaitReply> w, parse_tree* parse, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen, const char* command);

private:
    void            clearShardingKVS();

private:
    parse_tree*                                     mRedisParse;    /*  TODO::使用智能指针    */
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