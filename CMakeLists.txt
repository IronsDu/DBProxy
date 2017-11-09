cmake_minimum_required (VERSION 2.6)
project(dbproxy)

if(WIN32)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /std:c++latest")
elseif(UNIX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
endif()

set(net_src_list
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/net/EventLoop.cpp
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/net/CurrentThread.cpp
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/net/DataSocket.cpp
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/net/SocketLibFunction.c
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/net/TCPService.cpp
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/net/WrapTCPService.cpp
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/net/Connector.cpp
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/net/ListenThread.cpp
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/net/fdset.c
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/net/NetSession.cpp
)

set(timer_src_list
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/timer/Timer.cpp
)

set(base_src_list
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/utils/array.c
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/utils/buffer.c
)

set(utils_src_list
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/utils/base64.cpp
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/utils/md5calc.cpp
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/utils/SHA1.cpp
	${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/brynet/utils/ox_file.cpp
)

include_directories("${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/")
include_directories("${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/")
include_directories("${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/")
include_directories("${PROJECT_SOURCE_DIR}/3rdparty/brynet/src/")


set(lua_tinker_src_list
	${PROJECT_SOURCE_DIR}/3rdparty/lua_tinker/lua_tinker.cpp
	${PROJECT_SOURCE_DIR}/3rdparty/lua_tinker/lua_readtable.cpp
)

include_directories("${PROJECT_SOURCE_DIR}/3rdparty/lua_tinker")

set(lua_src_list
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lapi.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lauxlib.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lbaselib.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lcode.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/ldblib.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/ldebug.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/ldo.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/ldump.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lfunc.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lgc.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/linit.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/liolib.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/llex.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lmathlib.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lmem.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/loadlib.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lobject.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lopcodes.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/loslib.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lparser.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lstate.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lstring.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lstrlib.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/ltable.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/ltablib.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/ltm.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lundump.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lvm.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/lzio.c
	${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src/print.c
)
include_directories("${PROJECT_SOURCE_DIR}/3rdparty/luasrc/src")

set(proxy_src
	${PROJECT_SOURCE_DIR}/src/DBProxyServer.cpp
	${PROJECT_SOURCE_DIR}/src/Backend.cpp
	${PROJECT_SOURCE_DIR}/src/BaseWaitReply.cpp
	${PROJECT_SOURCE_DIR}/src/Client.cpp
	${PROJECT_SOURCE_DIR}/src/RedisWaitReply.cpp
	${PROJECT_SOURCE_DIR}/src/SSDBWaitReply.cpp
	${PROJECT_SOURCE_DIR}/src/protocol/SSDBProtocol.cpp
)

add_executable(dbproxy ${net_src_list} ${timer_src_list} ${base_src_list} ${lua_tinker_src_list} ${lua_src_list} ${utils_src_list} ${proxy_src})

if(WIN32)
  target_link_libraries(dbproxy ws2_32)
elseif(UNIX)
  find_package(Threads REQUIRED)
  target_link_libraries(dbproxy Threads::Threads)
endif()
