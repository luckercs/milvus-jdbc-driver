-- 标量过滤查询  filter limit output partition
SELECT f4_int32, f3_int16 FROM milvus.milvus_table_2
WHERE __partition__='part3' and id>5 and f2_int8=93  and f8_varchar='str_111'  limit 11

-- WHERE id>5 and f2_int8=93 and __partition__='part2' and f8_varchar='str_111' limit 11