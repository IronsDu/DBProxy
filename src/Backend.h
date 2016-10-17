#ifndef _BACKEND_CLIENT_H
#define _BACKEND_CLIENT_H

#include <memory>
#include <queue>
#include <vector>
#include <string>

#include "NetSession.h"

class BaseWaitReply;
struct parse_tree;
struct BackendParseMsg;

/*  链接db服务器的(网络线程-网络层)会话  */
class BackendSession : public BaseNetSession, public std::enable_shared_from_this<BackendSession>
{
public:
    BackendSession();
    ~BackendSession();

    void            forward(std::shared_ptr<BaseWaitReply>& waitReply, std::shared_ptr<std::string>& sharedStr, const char* b, size_t len);
    void            forward(std::shared_ptr<BaseWaitReply>& waitReply, std::shared_ptr<std::string>&& sharedStr, const char* b, size_t len);

    void            setID(int id);
    int             getID() const;

private:
    virtual size_t  onMsg(const char* buffer, size_t len) override;
    void            onEnter() override;
    void            onClose() override;

    void            processReply(parse_tree* redisReply, std::shared_ptr<std::string>& responseBinary, const char* replyBuffer, size_t replyLen);

private:
    parse_tree*                                 mRedisParse;
    std::shared_ptr<std::string>                mCache;

    std::queue<std::weak_ptr<BaseWaitReply>>    mPendingWaitReply;
    int                                         mID;
};

extern std::vector<std::shared_ptr<BackendSession>> gBackendClients;
std::shared_ptr<BackendSession>    findBackendByID(int id);

#endif