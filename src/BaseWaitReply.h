#ifndef _BASE_WAIT_REPLY_H
#define _BASE_WAIT_REPLY_H

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "Client.h"

class SSDBProtocolResponse;
struct parse_tree;

struct BackendParseMsg {
    using PTR = std::shared_ptr<BackendParseMsg>;

    std::shared_ptr<parse_tree> redisReply;
    std::shared_ptr<std::string> responseMemory;
};

class BaseWaitReply
{
public:
    using PTR = std::shared_ptr<BaseWaitReply>;
    using WEAK_PTR = std::weak_ptr<BaseWaitReply>;

    BaseWaitReply(const ClientSession::PTR& client);
    virtual ~BaseWaitReply();

    const ClientSession::PTR& getClient() const;

public:
    virtual void onBackendReply(brynet::net::TcpConnection::Ptr, const BackendParseMsg::PTR&) = 0;
    virtual void mergeAndSend(const ClientSession::PTR&) = 0;

public:
    bool isAllCompleted() const;
    void addWaitServer(brynet::net::TcpConnection::Ptr);

    bool hasError() const;
    void setError(const char* errorCode);

protected:
    struct PendingResponseStatus {
        brynet::net::TcpConnection::Ptr dbServerSocket;
        std::shared_ptr<std::string> responseBinary;
        std::shared_ptr<SSDBProtocolResponse> ssdbReply;
        std::shared_ptr<parse_tree> redisReply;
        bool forceOK = false;
    };

    std::vector<PendingResponseStatus> mWaitResponses;

    const ClientSession::PTR mClient;
    std::string mErrorCode;
};

#endif
