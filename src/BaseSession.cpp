#include "BaseSession.h"

BaseSession::BaseSession(brynet::net::TcpConnection::Ptr session)
    : mSession(session)
{
}

BaseSession::~BaseSession()
{}

brynet::net::EventLoop::Ptr BaseSession::getEventLoop() const
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
        session->send(data->c_str(), data->size());
    }
}

void BaseSession::send(const std::string& data)
{
    send(data.data(), data.size());
}

void BaseSession::send(const char* buffer, size_t len)
{
    auto session = getSession();
    if (session != nullptr)
    {
        session->send(buffer, len);
    }
}

brynet::net::TcpConnection::Ptr BaseSession::getSession() const
{
    return mSession.lock();
}
