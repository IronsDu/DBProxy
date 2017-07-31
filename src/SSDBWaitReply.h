#ifndef _SSDB_WAIT_REPLY_H
#define _SSDB_WAIT_REPLY_H

#include <vector>
#include <string>
#include <memory>

#include "BaseWaitReply.h"

class StrListSSDBReply : public BaseWaitReply
{
public:
    StrListSSDBReply(const ClientSession::PTR& client);
    void            pushStr(std::string&& str);
    void            pushStr(const std::string& str);
    void            pushStr(const char* str);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, const BackendParseMsg::PTR&);
    void            mergeAndSend(const ClientSession::PTR&);

private:
    SSDBProtocolRequest         mStrListResponse;
};

class SSDBSingleWaitReply : public BaseWaitReply
{
public:
    SSDBSingleWaitReply(const ClientSession::PTR& client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, const BackendParseMsg::PTR&);
    void            mergeAndSend(const ClientSession::PTR&);
};

class SSDBMultiSetWaitReply : public BaseWaitReply
{
public:
    SSDBMultiSetWaitReply(const ClientSession::PTR& client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, const BackendParseMsg::PTR&);
    void            mergeAndSend(const ClientSession::PTR&);
};

class SSDBMultiGetWaitReply : public BaseWaitReply
{
public:
    SSDBMultiGetWaitReply(const ClientSession::PTR& client);
private:
    virtual void    onBackendReply(int64_t dbServerSocketID, const BackendParseMsg::PTR&);
    void            mergeAndSend(const ClientSession::PTR&);
};

typedef SSDBMultiSetWaitReply SSDBMultiDelWaitReply;

#endif