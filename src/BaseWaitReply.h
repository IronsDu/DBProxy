#ifndef _BASE_WAIT_REPLY_H
#define _BASE_WAIT_REPLY_H

#include <string>
#include <memory>
#include <vector>

class ClientLogicSession;
class SSDBProtocolResponse;
struct parse_tree;

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
    void            setError(const char* errorCode);

protected:

    struct PendingResponseStatus
    {
        PendingResponseStatus()
        {
            dbServerSocketID = 0;
            responseBinary = nullptr;
            ssdbReply = nullptr;
            redisReply = nullptr;
            forceOK = false;
        }

        int64_t                 dbServerSocketID;       /*  此等待的response所在的db服务器的id    */
        std::string*            responseBinary;         /*  原始的(未解析)response报文 */
        SSDBProtocolResponse*   ssdbReply;              /*  解析好的ssdb response*/
        parse_tree*             redisReply;
        bool                    forceOK;                /*  是否强制设置成功    */
    };

    std::vector<PendingResponseStatus>  mWaitResponses; /*  等待的各个服务器返回值的状态  */

    ClientLogicSession*                 mClient;
    std::string*                        mErrorCode;
};

#endif