#ifndef _BACKEND_CLIENT_H
#define _BACKEND_CLIENT_H

#include <brynet/net/EventLoop.hpp>
#include <brynet/net/TcpService.hpp>
#include <memory>
#include <queue>
#include <string>
#include <vector>

#include "BaseSession.h"

class BaseWaitReply;
struct parse_tree;
struct BackendParseMsg;

class BackendSession : public BaseSession, public std::enable_shared_from_this<BackendSession>
{
public:
    BackendSession(brynet::net::TcpConnection::Ptr session, int id);
    ~BackendSession() = default;

    void forward(
            const std::shared_ptr<BaseWaitReply>& waitReply,
            const std::shared_ptr<std::string>& sharedStr,
            const char* b,
            size_t len);
    int getID() const;

private:
    size_t onMsg(const char* buffer, size_t len) override;
    void onEnter() override;
    void onClose() override;

    void processReply(
            const std::shared_ptr<parse_tree>& redisReply,
            std::shared_ptr<std::string>& responseBinary,
            const char* replyBuffer,
            size_t replyLen);

private:
    const int mID;
    std::shared_ptr<parse_tree> mRedisParse;
    std::shared_ptr<std::string> mCache;

    std::queue<std::weak_ptr<BaseWaitReply>> mPendingWaitReply;
};

std::shared_ptr<BackendSession> randomServer();
std::shared_ptr<BackendSession> findBackendByID(int id);

#endif
