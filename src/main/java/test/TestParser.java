package test;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
//import com.parser.gen.MilvusSqlParserImpl;

public class TestParser {
    public static void main(String[] args) throws SqlParseException {
        String sql = "alter system UPLOAD JAR 'hdfs://ns1/test.jar'";
        String sql2 = "alter system upload jar 'hdfs://ns1/test.jar'";
        String sql3 = "show tables";
        String sql13 = "select id, vec,f6 from milvus.test_tbl /*+ properties(k1='v1', k2='v2'), index(ename), no_hash_join */  where  id < '5' limit 10";

        String sql4 = "select * from milvus.test_tbl";
        String sql5 = "select f1,f2 from milvus.test_tbl";


        SqlParser.Config sqlParserConfig = SqlParser.config()
//                .withParserFactory(MilvusSqlParserImpl.FACTORY)
                .withCaseSensitive(false)
                .withConformance(SqlConformanceEnum.DEFAULT);

        SqlParser sqlParser = SqlParser.create(sql4, sqlParserConfig);
        SqlNode sqlNode = sqlParser.parseQuery();
        System.out.println(sqlNode);



    }
}
