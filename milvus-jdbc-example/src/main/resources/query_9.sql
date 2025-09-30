SELECT id,f2_int8,f3_int16,f8_varchar FROM milvus_table_2
WHERE id>=5 and f2_int8>10 and __partition__='part2' limit 13