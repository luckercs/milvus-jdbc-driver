
#  Milvus-Api与Sql的对应关系梳理


## （1）milvus版本

```shell
milvus 2.3.x
```

## （2）milvus-sdk-java

```shell
2.3.7

# api分类 汇总 
database Operations
collection Operations
Partition Operations
Alias Operations
Index Operations
Vector Operations
vectors Queries Operations
users Operations
roles Operations
```

## （3）数据类型对应关系

| io.milvus.grpc.DataType | java.sql.JDBCType |
|:------------------------|:------------------|
| None                    | -                 |
| Bool                    | BOOLEAN           | 
| Int8                    | TINYINT           |
| Int16                   | SMALLINT          |
| Int32                   | INTEGER           |
| Int64                   | BIGINT            |
| Float                   | FLOAT             |
| Double                  | DOUBLE            |
| String                  | (VARCHAR)         |
| VarChar                 | VARCHAR           |
| Array                   | ARRAY             |
| JSON                    | -                 |
| BinaryVector            | (ARRAY)           |
| FloatVector             | (ARRAY)           |

## （4）索引-相似度指标-索引参数-搜索参数对应关系

| index           | metricType        | indexParams                                                               | searchParams                                                                 | search_limit | 
|:----------------|:------------------|:--------------------------------------------------------------------------|:-----------------------------------------------------------------------------|:-------------|
| FLAT            | L2/IP/COSINE      | -                                                                         | metric_type                                                                  |              |
| IVF_FLAT        | L2/IP/COSINE      | nlist[1, 65536]                                                           | nprobe[1, nlist]                                                             |              |
| GPU_IVF_FLAT    | L2/IP/COSINE      | nlist[1, 65536]                                                           | nprobe[1, nlist]                                                             | topK<=256    |
| IVF_SQ8         | L2/IP/COSINE      | nlist[1, 65536]                                                           | nprobe[1, nlist],max_empty_result_buckets[1, 65535]                          |              |
| IVF_PQ          | L2/IP/COSINE      | nlist[1, 65536],m,nbits[1, 16]                                            | nprobe[1, nlist],max_empty_result_buckets[1, 65535]                          |              |
| SCANN           | L2/IP/COSINE      | nlist[1, 65536],with_raw_data[True/False]                                 | nprobe[1, nlist],reorder_k[top_k, ∞],radius[1, nlist],range_filter[top_k, ∞] |              |
| GPU_IVF_PQ      | L2/IP/COSINE      | nlist[1, 65536],m,nbits[1, 16]                                            | nprobe[1, nlist]                                                             | topK<=1024   |
| HNSW            | L2/IP/COSINE      | M[2, 2048],efConstruction[1, int_max]                                     | ef[top_k, int_max]                                                           |              |
| BIN_FLAT        | Jaccard/Hamming   | -                                                                         | metric_type                                                                  |              |
| BIN_IVF_FLAT    | Jaccard/Hamming   | nlist[1, 65536]                                                           | nprobe[1, nlist]                                                             |              |
| DiskANN         | L2/IP             | -                                                                         | search_list[topk，int32_max]                                                  |              |
| GPU_CAGRA       | L2/IP/COSINE      | intermediate_graph_degree,graph_degree,build_algo,cache_dataset_on_device | itopk_size,search_width, min_iterations/max_iterations,team_size             |              |
| GPU_BRUTE_FORCE | L2/IP/COSINE      | -                                                                         | metric_type                                                                  |              |

## （5）初版支持sql语句示例

```mysql
-- 向量相似性查询
SELECT id, vec from com.test.test_tbl ORDER BY distance('metricType=L2','searchParams={"nprobe":10,"offset":0}')(vec,[[0.1,0.2,...]]) LIMIT 10;
-- SELECT milvus_similarity_search('database'='com.test','collection'='test_tbl','output'='id,vec','metricType'='L2','searchParams'='{"nprobe":10,"offset":0}','vectorfiled'='vec','vectors'=[[0.1,0.2,...]], 'topk'=10);

-- 向量相似性混合标量查询
SELECT id, vec from com.test.test_tbl where id in ['1','2','3'] ORDER BY distance('metricType=L2','searchParams={"nprobe":10,"offset":0}')(vec,[[0.1,0.2,...]]) LIMIT 10;
-- SELECT milvus_similarity_search('database'='com.test','collection'='test_tbl','output'='id,vec','metricType'='L2','searchParams'='{"nprobe":10,"offset":0}','vectorfiled'='vec','vectors'=[[0.1,0.2,...]], 'topk'=10, 'expr'="id in ['1','2','3']");

-- 向量相似性范围查询
SELECT id, vec from com.test.test_tbl ORDER BY distance('metricType=L2','searchParams={"nprobe":10,"offset":0,"radius":20,"range_filter":17}')(vec,[[0.1,0.2,...]]) LIMIT 10;
-- SELECT milvus_similarity_search('database'='com.test','collection'='test_tbl','output'='id,vec','metricType'='L2','searchParams'='{"nprobe":10,"offset":0,"radius":20,"range_filter":17}','vectorfiled'='vec','vectors'=[[0.1,0.2,...]], 'topk'=10);

-- 标量过滤查询
SELECT id, vec from com.test.test_tbl where id in ['1','2','3'] LIMIT 10;
-- SELECT milvus_query('database'='com.test','collection'='test_tbl','output'='id,vec','topk'=10, 'expr'="id in ['1','2','3']");

-- 标量聚合查询
SELECT count(*) from com.test.test_tbl;
-- SELECT milvus_query('database'='com.test','collection'='test_tbl','output'='count(*)'");

-- 关联常规数据源进行查询
select * from (SELECT id from com.test.test_tbl ORDER BY distance('metricType=L2','searchParams={"nprobe":10,"offset":0}')(vec,[[0.1,0.2,...]]) LIMIT 10) as a join mysql.tbl as b on a.id=b.id;


```

## （6）后期考虑支持sql语句示例

```mysql
-- 建库
CREATE DATABASE com.test;

-- 建表以及索引
CREATE TABLE if not exists com.test.test_tbl
(
    id  varchar(100)  comment "id",
    vec Array(Float32) comment "vec",
    CONSTRAINT check_length CHECK length(vector) = 128,
    PRIMARY KEY(id),
    VECTOR INDEX idx_vector_filed(vec,L2,SCANN,"{\"nlist\":1024}");
) comment "com.test tbl";

-- 添加索引
ALTER TABLE com.test.test_tbl ADD VECTOR INDEX idx_vector_filed vec TYPE SCANN
    (
    'metricType = L2',
    'indexParams = {"nlist":1024}',
    );

-- 插入
insert into com.test.test_tbl(id, vec) values('0',[0.1,0.2,...]);

-- 特征抽取函数封装
SELECT id from com.test.test_tbl ORDER BY distance('metricType=L2','searchParams={"nprobe":10,"offset":0}')(vec,feature(s3://xx)) LIMIT 10;


-- 多实例支持大规模,均衡
...
```

