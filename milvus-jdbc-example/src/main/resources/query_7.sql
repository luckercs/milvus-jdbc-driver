-- 标量过滤查询  filter limit  有问题，当前只能下推filter，不能下推limit
SELECT * FROM milvus.milvus_table_1
WHERE id>5 and f2_int8=93 limit 13