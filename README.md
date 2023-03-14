#### Introduction
yet another rocksdb based redis, let's call it `yedis`

#### Goal
- highly horizontal scalable redis
- fully data can be persistent
- paxos based consistent protocol


#### 说明
- zset member strlen(key + value) < page_size / 2
- 默认8位的score 大小
- degree >= 4, just for index_split, if not index_split will be complicated
#### TODO
- [x] 处理分裂如何记住当前需要分裂节点的位子
- [x] 是否要记住当前page的父节点
- [x] 实现search
- [x] leaf node需要前后指针
- [x] 测试util的Random和随机字符串的生成

#### 分裂算法
- 算法导论的分裂算法是在搜索路径中出现满节点的就开始分裂, 用来维护btree的平衡
- 算法４里是递归更新（从底向上分裂）

#### 格式说明
- [x] node_page中flag需要提前，不然不好写parser

#### 设计测试程序
- [x] random inseret
- [x] small page_size test
- [ ] graph

#### bugs
1. [x] current page_id will be wrong after page return back to memory, if not call SetPageID implicitly
2. [x] map erase will cause pointer destruct?
    - 并不会导致指针的析构 
3. [x] index要满的情况，leaf_split可能会导致，index要add 2次child，所以有可能导致index也会满掉的场景
4. [x] pin and unpin logic
5. [x] ParentID的准确性定义: 保证搜索路径上的parent_id的正确应该就足够了, 不能保证每个parent_id的正确性
6. [ ] 需要增加index_page的pretty print debug  
7. [x] BTreeSmallPageTest.RandomBigInsert failed
8. [ ] github-ci
9. [ ] replay tests
10. [x] pass BTreeSmallPageTest.LeafNodePrevAndNextTest

11. [ ] remove 删除page的是如何和物理page_id做对应, 比如删除了page_id = 3的page，那么page_id=5的实际文件offset要往前移动
12. [ ] index_remove test
13. [ ] remove the only one key case

#### 依赖说明
1. 依赖abseil
2. 依赖https://github.com/google/crc32c/

