#include <iostream>
#include <unordered_map>

#include "SocketLibFunction.h"
#include "ox_file.h"
#include "WrapLog.h"
#include "Backend.h"
#include "Client.h"

using namespace std;

WrapLog::PTR gDailyLogger;
string sharding_function;

bool sharding_key(const char* str, int len, int& serverID)
{
    serverID = 0;//lua_tinker::call<int>(L, sharding_function.c_str(), string(str, len));   /*使用string是怕str没有结束符*/
    return true;
}

int main()
{
    int listenPort;         /*代理服务器的监听端口*/
    
    std::vector<std::tuple<int, string, int>> backendConfigs;
    {
        listenPort = 9999;

	backendConfigs.push_back(std::make_tuple(0, "127.0.0.1", 6379));
    }

    gDailyLogger = std::make_shared<WrapLog>();

    spdlog::set_level(spdlog::level::info);

    ox_dir_create("logs");
    ox_dir_create("logs/DBProxyServer");
    gDailyLogger->setFile("", "logs/DBProxyServer/daily");

    EventLoop mainLoop;

    WrapServer::PTR server = std::make_shared<WrapServer>();

    /*开启网络线程*/
    server->startWorkThread(1, [&](EventLoop&){
        syncNet2LogicMsgList(mainLoop);
    });

    /*链接数据库服务器*/
    for (auto& v : backendConfigs)
    {
        int id = std::get<0>(v);
        string ip = std::get<1>(v);
        int port = std::get<2>(v);

        gDailyLogger->info("connec db server id:{}, address: {}:{}", id, ip, port);
        sock fd = ox_socket_connect(ip.c_str(), port);
        auto bserver = std::make_shared<BackendLogicSession>();
        bserver->setID(id);
        WrapAddNetSession(server, fd, make_shared<BackendExtNetSession>(bserver), -1);
    }

    gDailyLogger->info("listen proxy port:{}", listenPort);
    /*开启代理服务器监听*/
    server->getListenThread().startListen(listenPort, nullptr, nullptr, [&](int fd){
        WrapAddNetSession(server, fd, make_shared<ClientExtNetSession>(std::make_shared<ClientLogicSession>()), -1);
    });

    gDailyLogger->warn("db proxy server start!");

    while (true)
    {
        mainLoop.loop(1);
        /*  处理网络线程投递过来的消息 */
        procNet2LogicMsgList();
        server->getService()->flushCachePackectList();
    }

    std::cin.get();


    return 0;
}
