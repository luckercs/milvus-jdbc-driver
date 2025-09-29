# 执行milvus-sql
sqlline -d com.milvus.jdbc.Driver -u jdbc:milvus://localhost:19530 -n root -p milvus

# 查看所有集合
sqlline> !tables

# 标量查询
sqlline> select * from default.collection1

# 向量相似性查询
sqlline> select id, ann('vec', '[0.8882545, 0.628688, 0.8879052, 0.11532456]') as dist
from default.collection2 WHERE id>=5 order by dist desc limit 15
