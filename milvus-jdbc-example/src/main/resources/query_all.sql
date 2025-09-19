-- 标量全量查询  已支持
SELECT * from milvus.milvus_table_1;

SELECT * FROM milvus.milvus_table_1
WHERE id>5 and f2_int8=93;

SELECT id,f2_int8,f3_int16 FROM milvus.milvus_table_1
WHERE id>5 and f2_int8=93;

SELECT id,f2_int8,f3_int16,f8_varchar FROM milvus.milvus_table_2
WHERE id>=5 and f2_int8>10 and __partition__='part2';

SELECT id,f2_int8,f3_int16,f8_varchar FROM milvus.milvus_table_1
WHERE id>5 and f8_varchar IN ('str_42', 'str_115', 'str_116') and f2_int8=93;

SELECT * from milvus.milvus_table_1 limit 13;

SELECT * FROM milvus.milvus_table_1
WHERE id>5 and f2_int8=93 limit 13;