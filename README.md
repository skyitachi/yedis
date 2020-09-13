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
- [ ] 处理分裂如何记住当前需要分裂节点的位子
- [ ] 是否要记住当前page的父节点