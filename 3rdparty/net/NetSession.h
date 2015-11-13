#ifndef _NETSESSION_H
#define _NETSESSION_H

#include <iostream>
using namespace std;

#include "WrapTCPService.h"

/*应用服务器的网络层会话对象基类*/
class BaseNetSession
{
public:
    typedef std::shared_ptr<BaseNetSession>  PTR;

    BaseNetSession()
    {
        mServer = nullptr;
    }

    virtual ~BaseNetSession()
    {
        mSocketID = 0;
    }

    void    setSession(WrapServer::PTR server, int64_t socketID, const string& ip)
    {
        mServer = server;
        mSocketID = socketID;
        mIP = ip;
    }

    WrapServer::PTR getServer()
    {
        return mServer;
    }

    /*处理收到的数据*/
    virtual int     onMsg(const char* buffer, int len) = 0;
    /*链接建立*/
    virtual void    onEnter() = 0;
    /*链接断开*/
    virtual void    onClose() = 0;

    const std::string&  getIP() const
    {
        return mIP;
    }

    int64_t         getSocketID() const
    {
        return mSocketID;
    }

    void            postClose()
    {
        mServer->getService()->disConnect(mSocketID);
    }

    void            sendPacket(const char* data, int len)
    {
        mServer->getService()->send(mSocketID, DataSocket::makePacket(data, len));
    }

    void            sendPacket(const DataSocket::PACKET_PTR& packet)
    {
        mServer->getService()->send(mSocketID, packet);
    }

    EventLoop*      getEventLoop()
    {
        return mServer->getService()->getEventLoopBySocketID(mSocketID);
    }
private:
    std::string         mIP;
    WrapServer::PTR     mServer;
    int64_t             mSocketID;
};

void WrapAddNetSession(WrapServer::PTR server, int fd, BaseNetSession::PTR pClient, int pingCheckTime);

#endif