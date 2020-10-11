#### Introduction
yet another rocksdb based redis, let's call it `yedis`

#### Goal
- highly horizontal scalable redis
- fully data can be persistent
- paxos based consistent protocol


#### 说明
- zset member strlen(key + value) < page_size / 2
- 默认8位的score 大小

#### TODO
- [x] 处理分裂如何记住当前需要分裂节点的位子
- [x] 是否要记住当前page的父节点
- [x] 实现search
- [ ] leaf node需要前后指针

#### 分裂算法
- 算法导论的分裂算法是在搜索路径中出现满节点的就开始分裂, 用来维护btree的平衡
- 算法４里是递归更新（从底向上分裂）

#### 格式说明
- [x] node_page中flag需要提前，不然不好写parser

#### 设计测试程序