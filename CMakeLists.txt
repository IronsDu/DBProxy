cmake_minimum_required (VERSION 2.6)
project(dbproxy)

if(WIN32)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /std:c++latest")
elseif(UNIX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
endif()

if(UNIX)
find_library(BRYNET_LIBRARY brynet)
find_library(SOL_LIBRARY sol)
find_library(LUA_LIBRARY lua)
endif()

set(proxy_src
	${PROJECT_SOURCE_DIR}/src/DBProxyServer.cpp
	${PROJECT_SOURCE_DIR}/src/BaseSession.cpp
	${PROJECT_SOURCE_DIR}/src/Backend.cpp
	${PROJECT_SOURCE_DIR}/src/BaseWaitReply.cpp
	${PROJECT_SOURCE_DIR}/src/Client.cpp
	${PROJECT_SOURCE_DIR}/src/RedisWaitReply.cpp
	${PROJECT_SOURCE_DIR}/src/SSDBWaitReply.cpp
	${PROJECT_SOURCE_DIR}/src/protocol/SSDBProtocol.cpp
)

add_executable(dbproxy ${proxy_src})

if(WIN32)
  target_link_libraries(dbproxy ws2_32 ${LUA_LIBRARY} ${BRYNET_LIBRARY})
elseif(UNIX)
  find_package(Threads REQUIRED)
  target_link_libraries(dbproxy Threads::Threads ${LUA_LIBRARY} ${BRYNET_LIBRARY})
endif()
