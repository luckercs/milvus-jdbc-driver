-- 标量过滤查询  filter未匹配回退 project  已支持
SELECT id,f2_int8,f3_int16,f8_varchar FROM milvus.milvus_table_2
WHERE id>=5 and f2_int8>10 and __partition__='part2'