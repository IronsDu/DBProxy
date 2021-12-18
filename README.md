# DBProxy
## 介绍
dbproxy是一个采用C++11编写的代理服务器（不过现在已经使用了一些C++17特性），支持[redis](https://github.com/antirez/redis)和 [ssdb](https://github.com/ideawu/ssdb)数据库。
其主要用于扩容和提高系统负载。使用lua控制sharding，把不同的key-value映射到不同的后端redis或ssdb服务器。

## 构建
dbproxy支持windwos和linux，需要支持 C++ 17的编译器，且需要使用`vcpkg`安装`brynet`、`lua`、`sol`.
如果使用vcpkg，则可以直接打开文件夹工程即可构建！

## 配置文件
dbproxy的配置文件是[Config.lua](https://github.com/IronsDu/DBProxy/blob/master/Config.lua)
其`ProxyConfig`的`backends`key配置后端服务器列表，其中的`sharding_function`指示sharding函数。
作为示例，`test_sharding`就是被指定的sharding函数，其根据key参数，返回对应的服务器号，这里返回0，意思是将key映射到`127.0.0.1` : `6379`这个服务器。

## 补充
目前dbproxy只作为代理映射，不包含读写分离以及额外缓存，也不解决分布式等问题。
当然其服务器C++代码主体并不涉及任何sharding方案，必须由用户自己在Config.lua里自己实现sharding函数 （当然，也可以从网上找现成的，譬如lua版的一致性hash [lua-consistent-hash](https://github.com/jaderhs/lua-consistent-hash))

## 感谢
一定程度上借鉴了[redis-shatter](https://github.com/fuzziqersoftware/redis-shatter)和[codis](https://github.com/wandoulabs/codis)。
