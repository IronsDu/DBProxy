#ifndef _REDIS_WAIT_REPLY_H
#define _REDIS_WAIT_REPLY_H

#include "BaseWaitReply.h"

class RedisSingleWaitReply : public BaseWaitReply
{
public:
    RedisSingleWaitReply(std::shared_ptr<ClientSession>& client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(std::shared_ptr<ClientSession>&);
};

class RedisStatusReply : public BaseWaitReply
{
public:
    RedisStatusReply(std::shared_ptr<ClientSession>& client, const char* status);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(std::shared_ptr<ClientSession>&);

private:
    std::string     mStatus;
};

class RedisErrorReply : public BaseWaitReply
{
public:
    RedisErrorReply(std::shared_ptr<ClientSession>& client, const char* error);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(std::shared_ptr<ClientSession>&);

private:
    std::string     mErrorCode;
};

class RedisWrongTypeReply : public BaseWaitReply
{
public:
    RedisWrongTypeReply(std::shared_ptr<ClientSession>& client, const char* wrongType, const char* detail);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(std::shared_ptr<ClientSession>&);

private:
    std::string     mWrongType;
    std::string     mWrongDetail;
};

class RedisMgetWaitReply : public BaseWaitReply
{
public:
    RedisMgetWaitReply(std::shared_ptr<ClientSession>& client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(std::shared_ptr<ClientSession>&);
};

class RedisMsetWaitReply : public BaseWaitReply
{
public:
    RedisMsetWaitReply(std::shared_ptr<ClientSession>& client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(std::shared_ptr<ClientSession>&);
};

class RedisDelWaitReply : public BaseWaitReply
{
public:
    RedisDelWaitReply(std::shared_ptr<ClientSession>& client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(std::shared_ptr<ClientSession>&);
};

#endif