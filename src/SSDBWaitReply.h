#ifndef _SSDB_WAIT_REPLY_H
#define _SSDB_WAIT_REPLY_H

#include <vector>
#include <string>
#include <memory>

#include "BaseWaitReply.h"

class StrListSSDBReply : public BaseWaitReply
{
public:
    StrListSSDBReply(std::shared_ptr<ClientSession>& client);
    void            pushStr(std::string&& str);
    void            pushStr(const std::string& str);
    void            pushStr(const char* str);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(std::shared_ptr<ClientSession>&);

private:
    SSDBProtocolRequest         mStrListResponse;
};

class SSDBSingleWaitReply : public BaseWaitReply
{
public:
    SSDBSingleWaitReply(std::shared_ptr<ClientSession>& client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(std::shared_ptr<ClientSession>&);
};

class SSDBMultiSetWaitReply : public BaseWaitReply
{
public:
    SSDBMultiSetWaitReply(std::shared_ptr<ClientSession>& client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(std::shared_ptr<ClientSession>&);
};

class SSDBMultiGetWaitReply : public BaseWaitReply
{
public:
    SSDBMultiGetWaitReply(std::shared_ptr<ClientSession>& client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, BackendParseMsg&);
    void            mergeAndSend(std::shared_ptr<ClientSession>&);
};

typedef SSDBMultiSetWaitReply SSDBMultiDelWaitReply;

#endif