#### Introduction
yet another rocksdb based redis, let's call it `yedis`

#### Goal
- highly horizontal scalable redis
- fully data can be persistent
- paxos based consistent protocol


### 说明
- zset member strlen(key + value) < page_size / 2