package com.test;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 仅 milvus 数据源查询
 */
public class MilvusJdbcDemo {

    private static final Logger LOG = LoggerFactory.getLogger(MilvusJdbcDemo.class);

    private static final String MILVUS_JDBC_DRIVER = "com.milvus.jdbc.Driver";
    private static final String MILVUS_JDBC_URL = "jdbc:milvus://localhost:19530/default";

    private static final String USER = "root";
    private static final String PASS = "Milvus";


    public static void main(String[] args) throws Exception {
        Class.forName(MILVUS_JDBC_DRIVER);
        Connection connection = DriverManager.getConnection(MILVUS_JDBC_URL, USER, PASS);

//        String sql = "SHOW tables";   // 表查询
//        String sql = "select * from milvus.milvus_table_1";  // 全量查询
//        String sql = "select * from milvus.milvus_table_1 where id>5 and f8_varchar in ('red', 'green', 'blue') and f2_int8=93  limit 11";
//        String sql = "select * from milvus.milvus_table_1 where f2_int8<93 and f3_int16>0 limit 11";  // 支持下推
//        String sql = "select f4_int32, f3_int16 from milvus.milvus_table_1 where f2_int8<93 and f3_int16>0 limit 11";  // 支持下推

//        String sql = "select * from milvus.milvus_table_2 ";  // 支持下推

        String sql = "select f4_int32, f3_int16 from milvus.milvus_table_2 where f2_int8<93 and f3_int16>0 and __partition__='part2' limit 11";  // 支持下推



//        String sql = "select f3_int16, f4_int32 from milvus.milvus_table_1 limit 11";  // 无法捕获
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        printResultSet(resultSet);
    }

    public static void printResultSet(ResultSet resultSet) throws Exception {
        int columnCount = resultSet.getMetaData().getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = resultSet.getMetaData().getColumnName(i);
            System.out.print(columnName + "  ");
        }
        System.out.println("");

        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(resultSet.getString(i) + "  ");
            }
            System.out.println("");
        }
    }

    public static void printPlan(CalciteConnection calciteConnection, String sql) throws Exception {
        CalciteServerStatement calciteServerStatement = calciteConnection.createStatement().unwrap(CalciteServerStatement.class);
        CalcitePrepare.Context prepareContext = calciteServerStatement.createPrepareContext();
        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder().parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
                .defaultSchema(prepareContext.getRootSchema().plus())
                .build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse(sql);
        SqlNode sqlNodeValidate = planner.validate(sqlNode);
        RelRoot relRoot = planner.rel(sqlNodeValidate);
        RelNode rel = relRoot.rel;
        printRelNode(rel);
    }

    public static void printRelNode(RelNode relNode) {
//        System.out.println(RelOptUtil.toString(relNode, SqlExplainLevel.ALL_ATTRIBUTES));
        System.out.println(RelOptUtil.toString(relNode));
        System.out.println("==============");
        System.out.println();
    }
}
