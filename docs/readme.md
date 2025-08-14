# milvus-jdbc-driver

milvus-jdbc驱动程序, 支持sql形式查询milvus

## (1) TODO
- jdbc-com.server
- jdbc-client

## (2) data-struct

| milvus            | jdbc             | note   | 
|-------------------|------------------|--------|
| Bool              | BOOLEAN          |        |
| Int8              | TINYINT          |        | 
| Int16             | SMALLINT         |        |
| Int32             | INTEGER          |        |
| Int64             | BIGINT           |        |
| Float             | FLOAT            |        |
| Double            | DOUBLE           |        |
| String            | VARCHAR          |        |
| VarChar           | VARCHAR          |        |
| Array             | Array            |        |
| JSON              | VARCHAR          |        |
| BinaryVector      | Array[TINYINT]   | byte[] |
| FloatVector       | Array[FLOAT]     |        |
| Float16Vector     | Array[TINYINT]   | byte[] |
| BFloat16Vector    | Array[TINYINT]   | byte[] |
| SparseFloatVector | Map<Long, Float> |        |


## (3) Thanks

如果这个项目对你有帮助，欢迎扫码打赏！

<img src="images/coffee.png" alt="coffee" width="200" height="200">

感谢你的慷慨解囊，你的支持是我前进的动力！
