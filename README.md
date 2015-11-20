# DBProxy
##介绍
dbproxy是一个采用C++11编写的代理服务器，支持[redis](https://github.com/antirez/redis)和 [ssdb](https://github.com/ideawu/ssdb)数据库。
其主要用于扩容和提高系统负载。使用lua控制sharding，把不同的key-value映射到不同的后端redis或ssdb服务器。
##构建
dbproxy支持windwos和linux。
* windows：打开根目录的DBProxy.sln编译即可。
* linux：
  * 1 : 在项目根目录执行 `cd 3rdparty/luasrc/src/` 命令进入lua src目录，然后执行 `make generic` 构建`liblua.so`
  * 2 : 回到项目根目录执行 `cp 3rdparty/luasrc/src/liblua.so .` 将`liblua.so`拷贝到当前目录。
  * 3 : 继续在根目录下执行 `make server` 构建 `dbserver` 即可。

##配置文件
dbproxy的配置文件是[Config.lua](https://github.com/IronsDu/DBProxy/blob/master/Config.lua)
其`ProxyConfig`的`backends`key配置后端服务器列表，其中的`sharding_function`指示sharding函数。
作为示例，`test_sharding`就是被指定的sharding函数，其根据key参数，返回对应的服务器号，这里返回0，意思是将key映射到`127.0.0.1` : `6379`这个服务器。

##补充
目前dbproxy只作为代理映射，不包含读写分离以及额外缓存，也不解决分布式等问题。
当然其服务器C++代码主体并不涉及任何sharding方案，必须由用户自己在Config.lua里自己实现sharding函数 （当然，也可以从网上找现成的，譬如lua版的一致性hash [lua-consistent-hash](https://github.com/jaderhs/lua-consistent-hash))

##感谢
一定程度上借鉴了[redis-shatter](https://github.com/fuzziqersoftware/redis-shatter)和[codis](https://github.com/wandoulabs/codis)。
