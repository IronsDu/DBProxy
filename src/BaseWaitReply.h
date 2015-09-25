#ifndef _BASE_WAIT_REPLY_H
#define _BASE_WAIT_REPLY_H

#include <string>
#include <memory>
#include <vector>

class ClientLogicSession;
class SSDBProtocolResponse;

class BaseWaitReply
{
public:
    typedef std::shared_ptr<BaseWaitReply>  PTR;
    typedef std::weak_ptr<BaseWaitReply>    WEAK_PTR;

    BaseWaitReply(ClientLogicSession* client);
    ClientLogicSession*  getClient();

    virtual ~BaseWaitReply();
public:
    /*  收到db服务器的返回值 */
    virtual void    onBackendReply(int64_t dbServerSocketID, const char* buffer, int len) = 0;
    /*  当所有db服务器都返回数据后，调用此函数尝试合并返回值并发送给客户端  */
    virtual void    mergeAndSend(ClientLogicSession*) = 0;

public:
    /*  检测是否所有等待的db服务器均已返回数据    */
    bool            isAllCompleted() const;
    /*  添加一个等待的db服务器    */
    void            addWaitServer(int64_t serverSocketID);

    bool            hasError() const;

    /*  设置出现错误    */
    void            setError();

protected:

    struct PendingResponseStatus
    {
        PendingResponseStatus()
        {
            dbServerSocketID = 0;
            reply = nullptr;
            ssdbReply = nullptr;
        }

        int64_t                 dbServerSocketID;       /*  此等待的response所在的db服务器的id    */
        std::string*            reply;                  /*  response */
        SSDBProtocolResponse*   ssdbReply;              /*  解析好的ssdb response*/
    };

    std::vector<PendingResponseStatus>  mWaitResponses; /*  等待的各个服务器返回值的状态  */

    ClientLogicSession*                 mClient;
    bool                                mIsError;       /*todo::使用字符串作为错误码*/
};

#endif