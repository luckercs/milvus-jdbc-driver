-- 标量过滤查询  filter limit  已支持
SELECT * FROM milvus.milvus_table_1
WHERE id>5 and f2_int8=93 limit 13