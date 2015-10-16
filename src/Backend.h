#ifndef _BACKEND_CLIENT_H
#define _BACKEND_CLIENT_H

#include <memory>
#include <queue>
#include <vector>
#include <string>
#include "NetThreadSession.h"

class BaseWaitReply;
struct parse_tree;
class BackendLogicSession;
struct BackendParseMsg;

/*  链接db服务器的(网络线程-网络层)会话  */
class BackendExtNetSession : public ExtNetSession
{
public:
    BackendExtNetSession(std::shared_ptr<BackendLogicSession> logicSession);
    ~BackendExtNetSession();

private:
    virtual int     onMsg(const char* buffer, int len) override;
    void            processReply(parse_tree* redisReply, std::string* replyBinary, const char* replyBuffer, size_t replyLen);
private:
    parse_tree*     mRedisParse;
    std::string*    mCache;
    std::shared_ptr<BackendLogicSession>    mLogicSession;
};

class ClientLogicSession;

/*  链接db服务器的(逻辑线程-逻辑层)会话    */
class BackendLogicSession : public BaseLogicSession
{
public:
    BackendLogicSession();
    ~BackendLogicSession();
    void            pushPendingWaitReply(std::weak_ptr<BaseWaitReply>);
    void            setID(int id);
    int             getID() const;

    void            onReply(BackendParseMsg& netParseMsg);
private:
    virtual void    onEnter() override;
    virtual void    onClose() override;
    virtual void    onMsg(const char* buffer, int len) override;
    
private:
    std::queue<std::weak_ptr<BaseWaitReply>>    mPendingWaitReply;
    int                                         mID;
};

extern std::vector<BackendLogicSession*>    gBackendClients;

BackendLogicSession*    findBackendByID(int id);
#endif