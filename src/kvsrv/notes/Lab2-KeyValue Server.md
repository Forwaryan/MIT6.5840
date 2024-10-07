# Lab2:Key/Value Server





## Lab2A 无故障的Key/Server服务器

* `Client`

​	客户端通过`RPC`向`Server`端发起请求

* `Server`

​	`Server`端使用`map`来处理键值对，当有多个`Client`的时候需要给`RPC`接口加锁，保证互斥访问共享`map`



## Lab2B 带有丢弃消息的Key/Server服务器

* 有的时候可能会发生丢失消息的情况(例如说)