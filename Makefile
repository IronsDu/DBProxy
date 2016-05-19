source = src/Backend.cpp\
		src/BaseWaitReply.cpp\
		src/Client.cpp\
		src/DBProxyServer.cpp\
		src/RedisWaitReply.cpp\
		src/SSDBWaitReply.cpp\
		3rdparty/accumulation-dev/src/net/CurrentThread.cpp\
		3rdparty/accumulation-dev/src/net/DataSocket.cpp\
		3rdparty/accumulation-dev/src/net/EventLoop.cpp\
		3rdparty/accumulation-dev/src/net/NetSession.cpp\
		3rdparty/accumulation-dev/src/net/SocketLibFunction.c\
		3rdparty/accumulation-dev/src/net/TCPService.cpp\
		3rdparty/accumulation-dev/src/net/WrapTCPService.cpp\
		3rdparty/accumulation-dev/src/ssdb/SSDBProtocol.cpp\
		3rdparty/accumulation-dev/src/utils/buffer.c\
		3rdparty/accumulation-dev/src/utils/ox_file.cpp\
		3rdparty/accumulation-dev/src/utils/systemlib.c\
		3rdparty/accumulation-dev/src/timer/timer.cpp\
		3rdparty/lua_tinker/lua_tinker.cpp\
		3rdparty/lua_tinker/lua_readtable.cpp\

server:
	cd ./3rdparty/luasrc/src/;make generic;cp liblua.so ../../../
	g++ $(source) -I./3rdparty/luasrc/src -I./3rdparty/lua_tinker/ -I./3rdparty/accumulation-dev/src/ssdb -I./3rdparty/accumulation-dev/src/net -I./3rdparty/accumulation-dev/src/timer -I./3rdparty/accumulation-dev/src/utils -O3 -std=c++11 -L./ -llua -lpthread -lrt -Wl,-rpath=./ -o dbproxy
