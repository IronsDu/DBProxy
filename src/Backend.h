#ifndef _BACKEND_CLIENT_H
#define _BACKEND_CLIENT_H

#include <memory>
#include <queue>
#include <vector>
#include <string>
#include "NetThreadSession.h"

class BaseWaitReply;
struct parse_tree;

/*  链接db服务器的(网络线程-网络层)会话  */
class BackendExtNetSession : public ExtNetSession
{
public:
    BackendExtNetSession(BaseLogicSession::PTR logicSession);
    ~BackendExtNetSession();

private:
    virtual int     onMsg(const char* buffer, int len) override;

private:
    parse_tree*     mRedisParse;
    std::string     mCache;
};

class ClientLogicSession;

/*  链接db服务器的(逻辑线程-逻辑层)会话    */
class BackendLogicSession : public BaseLogicSession
{
public:
    void            pushPendingWaitReply(std::weak_ptr<BaseWaitReply>);
    void            setID(int id);
    int             getID() const;

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