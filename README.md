# 简介

在 MIT-8.624-Raft 中实现了 Raft 库；

在 KVRaft 中实现了使用 Raft 做一致性保证的分布式 KV 服务；

本部分以上面两个库为基础实现了支持分片的分布式 KV 存储系统 shardkv，核心包含以下部分：
- shardkv controller
  - 本身也是一个分布式 KV 服务，它存储的是 shardkv 的一些配置信息（比如分片和 group 之间的映射关系，类似于 dragonfly 二开中的 Meta 元数据组件）；
  - 提供了 Query、Join、Leave、Move 方法来控制分片和 Group 之间的关系，然后通过 raft 模块进行各个节点之间的状态同步；
  - 当 raft 模块状态同步完成之后，节点会发送已经 commit 的日志，然后后台 apply goroutine 进行处理，主要是将用户的操作持久化到状态机中。
- shardkv server

shardkv 需要定时从 shard controller 拉取最新配置，然后根据配置来确定哪些 shard 需要迁移和删除，并在 Leader 节点上进行操作，最后通过配置变更通知集群中所有 follower。
shardkv 是由多个 Replica Group 组成，每个 Replica Group 负责一部分 shard 的读写请求，每个 Replica Group 又是一个 raft 集群，所有的 Group 组合到一起就是一个完整的 shardkv 服务。

实验中要求：
1. 一次最多只能处理一个配置，而且必须保证配置按照顺序进行处理，主要是为了避免覆盖正在进行中的配置变更任务；
2. KVServer 在 Get、Put、Append 方法和配置变更同时发生时仍然需要有一致性行为；
3. 在分片迁移期间，需要过滤重复请求，保证线性一致性；
4. 在分片迁移至其他 group 之后，源 group 的分片应该异步删除；
5. 在不同 group 之间发送分片数据时一般不会持有 map 的锁，如果一个分片数据发送到了另一个 group 但是当前 group 又要修改这个分片的数据时，就产生了竞态条件，一种解决方案是深拷贝一份；


整体交互图如下所示：

![image](https://github.com/1055373165/ShardKV/assets/33158355/c0aeba7b-0ce2-4c16-9982-e04e15d052ce)

代码可通过实验测试，测试结果如下
![](resources/2024-02-26-22-29-49.png)
