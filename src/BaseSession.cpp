#include "BaseSession.h"

BaseSession::BaseSession(brynet::net::DataSocket::PTR session)
    :
    mSession(session)
{
}

BaseSession::~BaseSession()
{}

brynet::net::EventLoop::PTR BaseSession::getEventLoop() const
{
    auto session = getSession();
    if (session == nullptr)
    {
        return nullptr;
    }
    return session->getEventLoop();
}

void BaseSession::send(const std::shared_ptr<std::string>& data)
{
    auto session = getSession();
    if (session != nullptr)
    {
        session->send(data);
    }
}

void BaseSession::send(const std::string& data)
{
    send(data.data(), data.size());
}

void BaseSession::send(const char * buffer, size_t len)
{
    auto session = getSession();
    if (session != nullptr)
    {
        session->send(buffer, len);
    }
}

brynet::net::DataSocket::PTR BaseSession::getSession() const
{
    return mSession.lock();
}
