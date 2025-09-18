-- 标量过滤查询 filter limit  limit没有下推
SELECT * FROM milvus.milvus_table_1
WHERE id>5 and f8_varchar IN ('red', 'green', 'blue') and f2_int8=93 limit 11