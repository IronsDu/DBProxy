#ifndef _LOGIC_NET_SESSION_H
#define _LOGIC_NET_SESSION_H

#include <stdint.h>
#include "WrapTCPService.h"

class BaseLogicSession
{
public:
    typedef std::shared_ptr<BaseLogicSession> PTR;

    BaseLogicSession()
    {
    }

    virtual ~BaseLogicSession()
    {}

    void            setSession(WrapServer::PTR server, int64_t socketID, const std::string& ip)
    {
        mServer = server;
        mSocketID = socketID;
        mIP = ip;
    }

    virtual void    onEnter() = 0;
    virtual void    onClose() = 0;
    virtual void    onMsg(const char* buffer, int len) = 0;

    void            send(const char* buffer, int len)
    {
        mServer->getService()->send(mSocketID, DataSocket::makePacket(buffer, len));
    }

    void            send(const DataSocket::PACKET_PTR& packet)
    {
        mServer->getService()->send(mSocketID, packet);
    }

    int64_t         getSocketID() const
    {
        return mSocketID;
    }
private:
    WrapServer::PTR     mServer;
    int64_t             mSocketID;
    std::string         mIP;
};

#endif