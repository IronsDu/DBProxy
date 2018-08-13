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
    virtual void    onBackendReply(brynet::net::DataSocket::PTR dbServerSocket, const BackendParseMsg::PTR&) override;
    virtual void    mergeAndSend(const ClientSession::PTR&) override;

private:
    SSDBProtocolRequest         mStrListResponse;
};

class SSDBSingleWaitReply : public BaseWaitReply
{
public:
    SSDBSingleWaitReply(const ClientSession::PTR& client);
private:
    virtual void    onBackendReply(brynet::net::DataSocket::PTR dbServerSocket, const BackendParseMsg::PTR&) override;
    virtual void    mergeAndSend(const ClientSession::PTR&) override;
};

class SSDBMultiSetWaitReply : public BaseWaitReply
{
public:
    SSDBMultiSetWaitReply(const ClientSession::PTR& client);
private:
    virtual void    onBackendReply(brynet::net::DataSocket::PTR dbServerSocket, const BackendParseMsg::PTR&) override;
    virtual void    mergeAndSend(const ClientSession::PTR&) override;
};

class SSDBMultiGetWaitReply : public BaseWaitReply
{
public:
    SSDBMultiGetWaitReply(const ClientSession::PTR& client);
private:
    virtual void    onBackendReply(brynet::net::DataSocket::PTR dbServerSocket, const BackendParseMsg::PTR&) override;
    virtual void    mergeAndSend(const ClientSession::PTR&) override;
};

typedef SSDBMultiSetWaitReply SSDBMultiDelWaitReply;

#endif