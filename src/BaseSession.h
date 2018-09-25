#ifndef _BASE_SESSION_H
#define _BASE_SESSION_H

#include <memory>
#include <string>

#include <brynet/net/TCPService.h>

class BaseSession
{
public:
    typedef std::shared_ptr<BaseSession> PTR;

public:
    BaseSession(brynet::net::DataSocket::PTR session);
    virtual ~BaseSession();

    brynet::net::EventLoop::PTR             getEventLoop() const;

    void                                    send(const std::shared_ptr<std::string>& data);
    void                                    send(const std::string& data);
    void                                    send(const char* buffer, size_t len);
    brynet::net::DataSocket::PTR            getSession() const;

    virtual size_t                          onMsg(const char* buffer, size_t len) = 0;
    virtual void                            onEnter() = 0;
    virtual void                            onClose() = 0;

private:
    const std::weak_ptr<brynet::net::DataSocket> mSession;
};

#endif