# milvus-jdbc-driver

milvus-jdbc驱动程序, 支持sql形式查询milvus， 支持 2.5.x 等版本

## (1) Requirements
- jdk8

## (2) Get Started

```shell

# 默认导入所有库和集合
java -cp milvus2milvus-1.0.0.jar Milvus2Milvus --uri <SRC_MILVUS_URI> --token <SRC_MILVUS_TOKEN> --t_uri <TARGET_MILVUS_URI> --t_token <TARGET_MILVUS_TOKEN> --collections * 

# 导入指定库下的所有集合
java -cp milvus2milvus-1.0.0.jar Milvus2Milvus --uri <SRC_MILVUS_URI> --token <SRC_MILVUS_TOKEN> --t_uri <TARGET_MILVUS_URI> --t_token <TARGET_MILVUS_TOKEN> --collections test.*

# 导入指定集合
java -cp milvus2milvus-1.0.0.jar Milvus2Milvus --uri <SRC_MILVUS_URI> --token <SRC_MILVUS_TOKEN> --t_uri <TARGET_MILVUS_URI> --t_token <TARGET_MILVUS_TOKEN> --collections test.milvus1,test2.milvus2

```
## (3) Thanks

如果这个项目对你有帮助，欢迎扫码打赏！

<img src="images/coffee.png" alt="coffee" width="200" height="200">

感谢你的慷慨解囊，你的支持是我前进的动力！
