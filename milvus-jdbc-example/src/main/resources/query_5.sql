-- 标量过滤查询  filter未匹配回退 project  已支持
SELECT id,f2_int8,f3_int16,f8_varchar FROM milvus.milvus_table_1
WHERE id>5 and f8_varchar IN ('str_42', 'str_115', 'str_116') and f2_int8<=93