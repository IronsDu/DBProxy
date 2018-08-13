#include <iostream>

#include <brynet/net/Platform.h>
#include <brynet/net/EventLoop.h>
#include <brynet/net/SocketLibFunction.h>
#include <brynet/utils/ox_file.h>
#include <brynet/net/Platform.h>
#include <brynet/net/ListenThread.h>
#include <brynet/net/TCPService.h>
#include <brynet/utils/app_status.h>
#include <sol.hpp>

#include "Backend.h"
#include "Client.h"

using namespace std;
using namespace brynet::net;

static void OnSessionEnter(BaseSession::PTR session)
{
    session->onEnter();

    auto tcpSession = session->getSession();
    tcpSession->setDataCallback([session](const char* buffer, size_t len) {
        return session->onMsg(buffer, len);
    });

    tcpSession->setDisConnectCallback([session](const DataSocket::PTR& tcpSession) {
        session->onClose();
    });
}

int main(int argc, const char**argv)
{
    if (argc != 2)
    {
        std::cerr << "usage: path-to-config" << std::endl;
        exit(-1);
    }

    brynet::net::base::InitSocket();

    srand(static_cast<unsigned int>(time(nullptr)));

    int listenPort;         /*代理服务器的监听端口*/
    string shardingFunction;
    std::string luaConfigFile;
    std::vector<std::tuple<int, string, int>> backendConfigs;

    try
    {
        sol::state luaState;
        luaState.do_file(argv[1]);

        auto proxyConfig = luaState.get<sol::table>("ProxyConfig");

        luaConfigFile = argv[1];
        listenPort = proxyConfig["listenPort"];
        shardingFunction = proxyConfig["sharding_function"];
        sol::table backendList = proxyConfig["backends"];

        for (auto& v : backendList)
        {
            auto backend = v.second.as<sol::table>();
            int id = backend["id"];
            string dbServerIP = backend["ip"];
            int port = backend["port"];
            backendConfigs.push_back(std::make_tuple(id, dbServerIP, port));

            std::cout << "backend :" << id << ", ip:" << dbServerIP << ", port:" << port << endl;
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "exception:" << e.what() << endl;
        exit(-1);
    }

    auto tcpService = brynet::net::TcpService::Create();
    auto listenThread = ListenThread::Create();

    int netWorkerThreadNum = std::thread::hardware_concurrency();
    /*开启网络线程*/
    tcpService->startWorkerThread(netWorkerThreadNum, nullptr);

    /*链接数据库服务器*/
    for (auto& v : backendConfigs)
    {
        int id = std::get<0>(v);
        string ip = std::get<1>(v);
        int port = std::get<2>(v);

        auto fd = brynet::net::base::Connect(false, ip.c_str(), port);
        if (fd == SOCKET_ERROR)
        {
            std::cerr << "connect:" << ip << ":" << port << " failed";
            exit(-1);
        }
        auto socket = brynet::net::TcpSocket::Create(fd, false);
        socket->SocketNodelay();
        socket->SetRecvSize(1024 * 1024);
        socket->SetSendSize(1024 * 1024);

        auto enterCallback = [id](const DataSocket::PTR& session) {
            auto bserver = std::make_shared<BackendSession>(session, id);
            OnSessionEnter(bserver);
        };
        tcpService->addDataSocket(std::move(socket),
            brynet::net::TcpService::AddSocketOption::WithMaxRecvBufferSize(1024 * 1024),
            brynet::net::TcpService::AddSocketOption::WithEnterCallback(enterCallback));
    }

    /*开启代理服务器监听*/
    listenThread->startListen(false, "0.0.0.0", listenPort, [=](brynet::net::TcpSocket::PTR socket) {
        socket->SocketNodelay();
        socket->SetRecvSize(1024 * 1024);
        socket->SetSendSize(1024 * 1024);

        auto enterCallback = [=](const DataSocket::PTR& session) {
            sol::state state;
            state.do_file(luaConfigFile);
            auto client = std::make_shared<ClientSession>(session, std::move(state), shardingFunction);
            OnSessionEnter(client);
        };
        tcpService->addDataSocket(std::move(socket),
            brynet::net::TcpService::AddSocketOption::WithMaxRecvBufferSize(1024 * 1024),
            brynet::net::TcpService::AddSocketOption::WithEnterCallback(enterCallback));
    });

    while (true)
    {
        if (app_kbhit())
        {
            string input;
            std::getline(std::cin, input);

            if (input == "quit")
            {
                std::cerr << "You enter quit will exit proxy" << std::endl;
                break;
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    listenThread->stopListen();
    tcpService->stopWorkerThread();

    return 0;
}
