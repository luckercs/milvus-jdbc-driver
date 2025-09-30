-- 标量过滤查询 filter project 已支持
SELECT id,f2_int8,f3_int16 FROM milvus_table_1
WHERE id>5 and f2_int8=93