#include "platform.h"

#ifdef PLATFORM_WINDOWS
#include <stdlib.h>
#include <crtdbg.h>
#endif

#include <iostream>
#include <unordered_map>

#include "SocketLibFunction.h"
#include "ox_file.h"
#include "WrapLog.h"
#include "Backend.h"
#include "Client.h"

extern "C"
{
#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"
#include "luaconf.h"
};
#include "lua_tinker.h"
#include "lua_readtable.h"

using namespace std;

string sharding_function;

bool sharding_key(struct lua_State* L, const char* str, int len, int& serverID)
{
    serverID = lua_tinker::call<int>(L, sharding_function.c_str(), string(str, len));   /*使用string是怕str没有结束符*/
    return true;
}

struct lua_State* malloc_luaState()
{
    lua_State* L = luaL_newstate();
    luaopen_base(L);
    luaL_openlibs(L);
    /*TODO::由启动参数指定配置路径*/
    lua_tinker::dofile(L, "Config.lua");
    return L;
}

#if defined(_MSC_VER)
#include <conio.h>
#else
#include <termios.h>
#include <unistd.h>
#include <fcntl.h>
#endif

int     app_kbhit(void)
{
#if defined(_MSC_VER)
    return _kbhit();
#else
    struct termios oldt, newt;
    int ch;
    int oldf;

    tcgetattr(STDIN_FILENO, &oldt);
    newt = oldt;
    newt.c_lflag &= ~(ICANON | ECHO);
    tcsetattr(STDIN_FILENO, TCSANOW, &newt);
    oldf = fcntl(STDIN_FILENO, F_GETFL, 0);
    fcntl(STDIN_FILENO, F_SETFL, oldf | O_NONBLOCK);

    ch = getchar();

    tcsetattr(STDIN_FILENO, TCSANOW, &oldt);
    fcntl(STDIN_FILENO, F_SETFL, oldf);

    if (ch != EOF)
    {
        ungetc(ch, stdin);
        return 1;
    }

    return 0;
#endif
}

int main()
{
    {
        srand(static_cast<unsigned int>(time(nullptr)));
        struct lua_State* L = nullptr;
        int listenPort;         /*代理服务器的监听端口*/
        ox_socket_init();
        std::vector<std::tuple<int, string, int>> backendConfigs;
        {
            struct msvalue_s config(true);
            L = luaL_newstate();
            luaopen_base(L);
            luaL_openlibs(L);
            /*TODO::由启动参数指定配置路径*/
            lua_tinker::dofile(L, "Config.lua");
            aux_readluatable_byname(L, "ProxyConfig", &config);

            map<string, msvalue_s*>& allconfig = *config._map;
            listenPort = atoi(allconfig["listenPort"]->_str.c_str());
            sharding_function = allconfig["sharding_function"]->_str;

            map<string, msvalue_s*>& backends = *allconfig["backends"]->_map;

            for (auto& v : backends)
            {
                map<string, msvalue_s*>& oneBackend = *(v.second)->_map;
                int id = atoi(oneBackend["id"]->_str.c_str());
                string dbServerIP = oneBackend["ip"]->_str;
                int port = atoi(oneBackend["port"]->_str.c_str());
                backendConfigs.push_back(std::make_tuple(id, dbServerIP, port));
            }
        }

        //WrapLog::PTR gDailyLogger = std::make_shared<WrapLog>();

        spdlog::set_level(spdlog::level::info);

        ox_dir_create("logs");
        ox_dir_create("logs/DBProxyServer");
        //gDailyLogger->setFile("", "logs/DBProxyServer/daily");

        EventLoop mainLoop;

        WrapServer::PTR server = std::make_shared<WrapServer>();
        ListenThread::PTR listenThread = std::make_shared<ListenThread>();

        int netWorkerThreadNum = ox_getcpunum();
        /*开启网络线程*/
        server->startWorkThread(netWorkerThreadNum, nullptr);

        /*链接数据库服务器*/
        for (auto& v : backendConfigs)
        {
            int id = std::get<0>(v);
            string ip = std::get<1>(v);
            int port = std::get<2>(v);

            //gDailyLogger->info("connec db server id:{}, address: {}:{}", id, ip, port);
            sock fd = ox_socket_connect(ip.c_str(), port);
            auto bserver = std::make_shared<BackendSession>();
            bserver->setID(id);
            WrapAddNetSession(server, fd, bserver, -1);
        }

       // gDailyLogger->info("listen proxy port:{}", listenPort);
        /*开启代理服务器监听*/
        listenThread->startListen(listenPort, nullptr, nullptr, [&](int fd){
            WrapAddNetSession(server, fd, make_shared<ClientSession>(), -1);
        });

        //gDailyLogger->warn("db proxy server start!");

        while (true)
        {
            if (app_kbhit())
            {
                break;
            }

            mainLoop.loop(1);
        }

        listenThread->closeListenThread();
        server->getService()->closeService();
        lua_close(L);
        L = nullptr;
    }

    return 0;
}
