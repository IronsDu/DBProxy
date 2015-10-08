#ifndef _SSDB_WAIT_REPLY_H
#define _SSDB_WAIT_REPLY_H

#include <vector>
#include <string>
#include <memory>

#include "BaseWaitReply.h"

class StrListSSDBReply : public BaseWaitReply
{
public:
    StrListSSDBReply(ClientLogicSession* client);
    void            pushStr(std::string&& str);
    void            pushStr(const std::string& str);
    void            pushStr(const char* str);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(ClientLogicSession*);

private:
    SSDBProtocolRequest         mStrListResponse;
};

class SSDBSingleWaitReply : public BaseWaitReply
{
public:
    SSDBSingleWaitReply(ClientLogicSession* client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(ClientLogicSession*);
};

class SSDBMultiSetWaitReply : public BaseWaitReply
{
public:
    SSDBMultiSetWaitReply(ClientLogicSession* client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(ClientLogicSession*);
};

class SSDBMultiGetWaitReply : public BaseWaitReply
{
public:
    SSDBMultiGetWaitReply(ClientLogicSession* client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(ClientLogicSession*);
};

typedef SSDBMultiSetWaitReply SSDBMultiDelWaitReply;

#endif