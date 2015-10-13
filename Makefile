source = src/Backend.cpp\
		src/BaseWaitReply.cpp\
		src/Client.cpp\
		src/DBProxyServer.cpp\
		src/RedisWaitReply.cpp\
		src/SSDBWaitReply.cpp\
		3rdparty/net/buffer.c\
		3rdparty/net/CurrentThread.cpp\
		3rdparty/net/DataSocket.cpp\
		3rdparty/net/EventLoop.cpp\
		3rdparty/net/NetSession.cpp\
		3rdparty/net/NetThreadSession.cpp\
		3rdparty/net/SocketLibFunction.c\
		3rdparty/net/TCPService.cpp\
		3rdparty/net/WrapTCPService.cpp\
		3rdparty/ssdb/SSDBProtocol.cpp\
		3rdparty/utils/ox_file.cpp\
		3rdparty/utils/systemlib.c\
		3rdparty/utils/timer.cpp\
		3rdparty/luatinker/lua_tinker.cpp\
		3rdparty/utils/lua_readtable.cpp\

server:
	g++ $(source) -I./3rdparty/luasrc/src -I./3rdparty/luatinker/ -I./3rdparty/ssdb -I./3rdparty/net -I./3rdparty/spdlog/include -I./3rdparty/utils -O3 -std=c++11 -L./3rdparty/luasrc/src -llua -lpthread -o dbserver
