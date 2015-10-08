#ifndef _REDIS_WAIT_REPLY_H
#define _REDIS_WAIT_REPLY_H

#include "BaseWaitReply.h"

class RedisSingleWaitReply : public BaseWaitReply
{
public:
    RedisSingleWaitReply(ClientLogicSession* client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(ClientLogicSession*);
};

class RedisStatusReply : public BaseWaitReply
{
public:
    RedisStatusReply(ClientLogicSession* client, const char* status);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(ClientLogicSession*);

private:
    std::string     mStatus;
};

class RedisErrorReply : public BaseWaitReply
{
public:
    RedisErrorReply(ClientLogicSession* client, const char* error);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(ClientLogicSession*);

private:
    std::string     mErrorCode;
};

class RedisWrongTypeReply : public BaseWaitReply
{
public:
    RedisWrongTypeReply(ClientLogicSession* client, const char* wrongType, const char* detail);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(ClientLogicSession*);

private:
    std::string     mWrongType;
    std::string     mWrongDetail;
};

class RedisMgetWaitReply : public BaseWaitReply
{
public:
    RedisMgetWaitReply(ClientLogicSession* client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(ClientLogicSession*);
};

class RedisMsetWaitReply : public BaseWaitReply
{
public:
    RedisMsetWaitReply(ClientLogicSession* client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(ClientLogicSession*);
};

class RedisDelWaitReply : public BaseWaitReply
{
public:
    RedisDelWaitReply(ClientLogicSession* client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(ClientLogicSession*);
};

#endif