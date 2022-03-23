#include <brynet/base/AppStatus.hpp>
#include <brynet/base/Platform.hpp>
#include <brynet/net/EventLoop.hpp>
#include <brynet/net/ListenThread.hpp>
#include <brynet/net/SocketLibFunction.hpp>
#include <brynet/net/TcpService.hpp>
#include <brynet/net/wrapper/ConnectionBuilder.hpp>
#include <brynet/net/wrapper/ServiceBuilder.hpp>
#include <iostream>
#include <sol/sol.hpp>

#include "Backend.h"
#include "Client.h"

using namespace std;
using namespace brynet::net;

static void OnSessionEnter(BaseSession::PTR session)
{
    session->onEnter();
    auto tcpSession = session->getSession();
    tcpSession->setDataCallback([session](brynet::base::BasePacketReader& reader) {
        session->onMsg(reader.currentBuffer(), reader.getLeft());
        reader.consumeAll();
    });

    tcpSession->setDisConnectCallback([session](const TcpConnection::Ptr& tcpSession) {
        session->onClose();
    });
}

int main(int argc, const char** argv)
{
    if (argc != 2)
    {
        std::cerr << "usage: path-to-config" << std::endl;
        exit(-1);
    }

    brynet::net::base::InitSocket();

    srand(static_cast<unsigned int>(time(nullptr)));

    int listenPort;
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

        for (const auto& [_, v] : backendList)
        {
            auto backend = v.as<sol::table>();
            int id = backend["id"];
            string dbServerIP = backend["ip"];
            int port = backend["port"];
            backendConfigs.push_back({id, dbServerIP, port});

            std::cout << "backend :" << id << ", ip:" << dbServerIP << ", port:" << port << endl;
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "exception:" << e.what() << endl;
        exit(-1);
    }

    auto tcpService = brynet::net::IOThreadTcpService::Create();
    int netWorkerThreadNum = std::thread::hardware_concurrency();
    tcpService->startWorkerThread(netWorkerThreadNum, nullptr);

    wrapper::ListenerBuilder listener;
    listener.WithService(tcpService)
            .AddSocketProcess([](TcpSocket& socket) {
                socket.setNodelay();
            })
            .WithMaxRecvBufferSize(1024 * 1024)
            .WithAddr(false, std::string("0.0.0.0"), static_cast<size_t>(listenPort))
            .AddEnterCallback([=](const TcpConnection::Ptr& session) {
                sol::state state;
                state.do_file(luaConfigFile);
                auto client = std::make_shared<ClientSession>(session, std::move(state), shardingFunction);
                OnSessionEnter(client);
            })
            .asyncRun();

    auto connector = AsyncConnector::Create();
    connector->startWorkerThread();

    for (const auto& [id, ip, port] : backendConfigs)
    {
        auto enterCallback = [id](const TcpConnection::Ptr& session) {
            auto bserver = std::make_shared<BackendSession>(session, id);
            OnSessionEnter(bserver);
        };

        try
        {
            wrapper::ConnectionBuilder connectionBuilder;
            connectionBuilder
                    .WithService(tcpService)
                    .WithConnector(connector)
                    .WithMaxRecvBufferSize(1024 * 1024)
                    .AddEnterCallback(enterCallback)
                    .WithAddr(ip, port)
                    .asyncConnect();
        }
        catch (std::exception& e)
        {
            std::cout << "exception :" << e.what() << std::endl;
        }
    }

    while (true)
    {
        if (brynet::base::app_kbhit())
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

    listener.stop();
    tcpService->stopWorkerThread();

    return 0;
}
