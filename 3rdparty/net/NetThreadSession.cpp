#include "NetThreadSession.h"

MsgQueue<Net2LogicMsg>    gNet2LogicMsgList;

void ExtNetSession::pushDataMsgToLogicThread(const char* data, int len)
{
    pushDataMsg2LogicMsgList(mLogicSession, data, len);
}

int ExtNetSession::onMsg(const char* buffer, int len)
{
    return len;
}

void ExtNetSession::onEnter()
{
    mLogicSession->setSession(getServer(), getID(), getIP());
    Net2LogicMsg tmp(mLogicSession, Net2LogicMsgTypeEnter);
    gNet2LogicMsgList.Push(tmp);
}

void ExtNetSession::onClose()
{
    Net2LogicMsg tmp(mLogicSession, Net2LogicMsgTypeClose);
    gNet2LogicMsgList.Push(tmp);
}

void pushDataMsg2LogicMsgList(BaseLogicSession::PTR session, const char* data, int len)
{
    Net2LogicMsg tmp(session, Net2LogicMsgTypeData);
    tmp.setData(data, len);
    gNet2LogicMsgList.Push(tmp);
}

void syncNet2LogicMsgList(EventLoop& eventLoop)
{
    gNet2LogicMsgList.ForceSyncWrite();
    if (gNet2LogicMsgList.SharedListSize() > 0)
    {
        eventLoop.wakeup();
    }
}

void procNet2LogicMsgList()
{
    gNet2LogicMsgList.SyncRead(0);

    Net2LogicMsg msg;
    while (gNet2LogicMsgList.PopFront(&msg))
    {
        switch (msg.mMsgType)
        {
            case Net2LogicMsgTypeEnter:
            {
                msg.mSession->onEnter();
            }
            break;
            case Net2LogicMsgTypeData:
            {
                msg.mSession->onMsg(msg.mPacket.c_str(), msg.mPacket.size());
            }
            break;
            case Net2LogicMsgTypeClose:
            {
                msg.mSession->onClose();
            }
            break;
            default:
                break;
        }
    }
}