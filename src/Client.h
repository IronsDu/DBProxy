#ifndef _CLIENT_H
#define _CLIENT_H

#include <memory>
#include <deque>
#include <string>
#include "NetThreadSession.h"

struct parse_tree;
class BaseWaitReply;
class SSDBProtocolResponse;
class ClientLogicSession;

/*  代理服务器的客户端(网络线程)网络层会话    */
class ClientExtNetSession : public ExtNetSession
{
public:
    ClientExtNetSession(std::shared_ptr<ClientLogicSession> logicSession);

    ~ClientExtNetSession();

private:
    virtual int     onMsg(const char* buffer, int len) override;
    void            processRequest(bool isRedis, SSDBProtocolResponse* ssdbQuery, parse_tree* redisRequest, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen);

private:
    parse_tree*     mRedisParse;
    std::shared_ptr<std::string> mCache;
    std::shared_ptr<ClientLogicSession> mLogicSession;
};

/*  代理服务器的客户端(逻辑线程)逻辑层会话    */
class ClientLogicSession : public BaseLogicSession, public std::enable_shared_from_this<ClientLogicSession>
{
public:
    ClientLogicSession();
    /*  处理等待队列中已经完成的请求以及确认出错的请求 */
    void            processCompletedReply();
    void            onRequest(bool isRedis, SSDBProtocolResponse* ssdbQuery, parse_tree* redisRequest, std::shared_ptr<std::string>& requestBinary, const char* requestBuffer, size_t requestLen);
private:
    virtual void    onEnter() override;
    virtual void    onClose() override;
    virtual void    onMsg(const char* buffer, int len) override;

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
    std::deque<std::shared_ptr<BaseWaitReply>>      mPendingReply;
    bool                                            mNeedAuth;
    bool                                            mIsAuth;
    string                                          mPassword;
};

#endif