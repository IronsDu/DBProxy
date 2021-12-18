#ifndef _BASE_SESSION_H
#define _BASE_SESSION_H

#include <brynet/net/TcpService.hpp>
#include <memory>
#include <string>

class BaseSession
{
public:
    using PTR = std::shared_ptr<BaseSession>;

public:
    BaseSession(brynet::net::TcpConnection::Ptr session);
    virtual ~BaseSession();

    brynet::net::EventLoop::Ptr getEventLoop() const;

    void send(const std::shared_ptr<std::string>& data);
    void send(const std::string& data);
    void send(const char* buffer, size_t len);
    brynet::net::TcpConnection::Ptr getSession() const;

    virtual size_t onMsg(const char* buffer, size_t len) = 0;
    virtual void onEnter() = 0;
    virtual void onClose() = 0;

private:
    const std::weak_ptr<brynet::net::TcpConnection> mSession;
};

#endif
