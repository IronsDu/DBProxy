#include "BaseSession.h"

BaseSession::BaseSession(brynet::net::TcpConnection::Ptr session)
    : mSession(session)
{
}

BaseSession::~BaseSession()
{}


class SharedStringSendMsg : public brynet::net::SendableMsg
{
public:
    explicit SharedStringSendMsg(std::shared_ptr<std::string> msg)
        : mMsg(std::move(msg))
    {}
    explicit SharedStringSendMsg(const std::shared_ptr<std::string>& msg)
        : mMsg(msg)
    {}

    const void* data() override
    {
        return mMsg->data();
    }
    size_t size() override
    {
        return mMsg->size();
    }

private:
    std::shared_ptr<std::string> mMsg;
};

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
        session->send(std::make_shared<SharedStringSendMsg>(data));
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
