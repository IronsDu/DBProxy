#include <fstream>
#include <memory>
#include <iostream>
#include <shared_mutex>

#include "protocol/RedisParse.h"
#include "Client.h"
#include "protocol/SSDBProtocol.h"
#include "SSDBWaitReply.h"

#include "Backend.h"

using namespace std;

class ClientSession;
static std::vector<shared_ptr<BackendSession>>    gBackendClients;
std::mutex gBackendClientsLock;

BackendSession::BackendSession(brynet::net::DataSocket::PTR session, int id)
    :
    BaseSession(session),
    mID(id)
{
    cout << "BackendSession::BackendSession" << endl;
    mCache = nullptr;
}

BackendSession::~BackendSession()
{
    cout << "BackendSession::~BackendSession" << endl;
}

void BackendSession::onEnter()
{
    std::lock_guard<std::mutex> lock(gBackendClientsLock);
    gBackendClients.push_back(shared_from_this());
}

void BackendSession::onClose()
{
    {
        std::lock_guard<std::mutex> lock(gBackendClientsLock);
        const auto oldEnd = gBackendClients.end();
        const auto newEnd = std::remove_if(gBackendClients.begin(),
            gBackendClients.end(),
            [=](const shared_ptr<BackendSession>& v) {
                return v.get() == this;
            });
        if (newEnd != oldEnd)
        {
            gBackendClients.erase(newEnd, oldEnd);
        }
    }

    /*  当与db server断开后，对等待此服务器响应的客户端请求设置错误(返回给客户端)  */
    while (!mPendingWaitReply.empty())
    {
        std::shared_ptr<ClientSession> client = nullptr;
        auto wp = mPendingWaitReply.front().lock();
        if (wp != nullptr)
        {
            client = wp->getClient();
        }
        mPendingWaitReply.pop();

        if (client == nullptr)
        {
            continue;
        }

        auto eventLoop = client->getEventLoop();
        if (eventLoop == nullptr)
        {
            continue;
        }
        if (eventLoop->isInLoopThread())
        {
            wp->setError("backend error");
            client->processCompletedReply();
        }
        else
        {
            eventLoop->pushAsyncProc([clientCapture = std::move(client), wpCapture = std::move(wp)](){
                wpCapture->setError("backend error");
                clientCapture->processCompletedReply();
            });
        }
    }
}

size_t BackendSession::onMsg(const char* buffer, size_t len)
{
    size_t totalLen = 0;

    const char c = buffer[0];
    if (mRedisParse != nullptr ||
        !IS_NUM(c))
    {
        /*  redis reply */
        char* parseEndPos = (char*)buffer;
        char* parseStartPos = parseEndPos;

        while (totalLen < len)
        {
            if (mRedisParse == nullptr)
            {
                mRedisParse = std::shared_ptr<parse_tree>(parse_tree_new(), [](parse_tree* parse) {
                    parse_tree_del(parse);
                });
            }

            int parseRet = parse(mRedisParse.get(), &parseEndPos, (char*)buffer+len);
            totalLen += (parseEndPos - parseStartPos);

            if (parseRet == REDIS_OK)
            {
                if (mCache == nullptr)
                {
                    processReply(mRedisParse, mCache, parseStartPos, parseEndPos - parseStartPos);
                }
                else
                {
                    mCache->append(parseStartPos, parseEndPos - parseStartPos);
                    processReply(mRedisParse, mCache, mCache->c_str(), mCache->size());
                    mCache = nullptr;
                }

                parseStartPos = parseEndPos;
                mRedisParse = nullptr;
            }
            else if (parseRet == REDIS_RETRY)
            {
                if (mCache == nullptr)
                {
                    mCache.reset(new std::string);
                }
                mCache->append(parseStartPos, parseEndPos - parseStartPos);
                break;
            }
            else
            {
                break;
            }
        }
    }
    else
    {
        /*  ssdb reply    */
        char* parseStartPos = (char*)buffer;
        int leftLen = len;
        int packetLen = 0;
        while ((packetLen = SSDBProtocolResponse::check_ssdb_packet(parseStartPos, leftLen)) > 0)
        {
            processReply(nullptr, mCache, parseStartPos, packetLen);

            totalLen += packetLen;
            leftLen -= packetLen;
            parseStartPos += packetLen;
        }
    }

    return totalLen;
}

void BackendSession::processReply(const std::shared_ptr<parse_tree>& redisReply, 
    std::shared_ptr<std::string>& responseBinary, 
    const char* replyBuffer, 
    size_t replyLen)
{
    assert(!mPendingWaitReply.empty());
    if (mPendingWaitReply.empty())
    {
        return;
    }

    auto reply = mPendingWaitReply.front().lock();
    mPendingWaitReply.pop();
    if (reply == nullptr)
    {
        return;
    }

    auto client = reply->getClient();
    if (client == nullptr)
    {
        return;
    }

    auto netParseMsg = std::make_shared<BackendParseMsg>();
    netParseMsg->redisReply = redisReply;
    if (responseBinary != nullptr)
    {
        netParseMsg->responseMemory = responseBinary;
    }
    else
    {
        netParseMsg->responseMemory = std::make_shared<std::string>(replyBuffer, replyLen);
    }

    auto eventLoop = client->getEventLoop();
    if (eventLoop == nullptr)
    {
        return;
    }

    if (eventLoop->isInLoopThread())
    {
        if (netParseMsg->redisReply != nullptr && netParseMsg->redisReply->type == REDIS_REPLY_ERROR)
        {
            reply->setError(netParseMsg->redisReply->reply->str);
        }
        //ssdb会在mergeAndSend时处理错误/失败的response
        reply->onBackendReply(getSession(), netParseMsg);
        client->processCompletedReply();
    }
    else
    {
        // 投递到client所在线程去处理reply
        eventLoop->pushAsyncProc([clientCapture = std::move(client),
            netParseMsgCapture = std::move(netParseMsg),
            replyCapture = std::move(reply),
            session = getSession()](){

            if (netParseMsgCapture->redisReply != nullptr && netParseMsgCapture->redisReply->type == REDIS_REPLY_ERROR)
            {
                replyCapture->setError(netParseMsgCapture->redisReply->reply->str);
            }
            //ssdb会在mergeAndSend时处理错误/失败的response
            replyCapture->onBackendReply(session, netParseMsgCapture);

            clientCapture->processCompletedReply();
        });
    }
}

void BackendSession::forward(const std::shared_ptr<BaseWaitReply>& waitReply, 
    const std::shared_ptr<string>& sharedStr, 
    const char* b, 
    size_t len)
{
    auto tmp = sharedStr;
    if (sharedStr == nullptr)
    {
        tmp = std::make_shared<std::string>(b, len);
    }

    waitReply->addWaitServer(getSession());

    auto eventLoop = getEventLoop();
    if (eventLoop == nullptr)
    {
        return;
    }

    if (eventLoop->isInLoopThread())
    {
        mPendingWaitReply.push(waitReply);
        send(tmp);
    }
    else
    {
        eventLoop->pushAsyncProc([sharedThis = shared_from_this(), waitReply, sharedStrCaptupre = std::move(tmp)](){
            sharedThis->mPendingWaitReply.push(std::move(waitReply));
            sharedThis->send(sharedStrCaptupre);
        });
    }
}

int BackendSession::getID() const
{
    return mID;
}

std::shared_ptr<BackendSession> randomServer()
{
    std::lock_guard<std::mutex> lock(gBackendClientsLock);
    if (gBackendClients.empty())
    {
        return nullptr;
    }
    return gBackendClients[std::rand() % gBackendClients.size()];
}

shared_ptr<BackendSession> findBackendByID(int id)
{
    std::lock_guard<std::mutex> lock(gBackendClientsLock);

    auto it = std::find_if(gBackendClients.begin(), gBackendClients.end(), [=](const shared_ptr<BackendSession>& v) {
        return v->getID() == id;
    });
    if (it != gBackendClients.end())
    {
        return *it;
    }

    return nullptr;
}