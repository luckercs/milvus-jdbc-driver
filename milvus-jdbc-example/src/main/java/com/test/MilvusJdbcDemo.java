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

import java.sql.*;

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

//        String sql = "select f4_int32, f3_int16 from milvus.milvus_table_2 where f2_int8<93 and f3_int16>0 and __partition__='part2' limit 11";  // 支持下推
//        String sql = "select f3_int16, f4_int32 from milvus.milvus_table_1 limit 11";  // 无法捕获
        // String sql = "select f4_int32, f3_int16, ann('f11_floatvector', '[0.1,0.2]') as dist from milvus.milvus_table_1 where f2_int8<93 and f3_int16>0 order by dist limit 11";
        // String sql = "explain plan for select f4_int32, f3_int16, ann('f11_floatvector', '[0.1,0.2]') as dist from milvus.milvus_table_1 where f2_int8<93 and f3_int16>0 order by dist limit 11";
        // String sql = "select f4_int32, f3_int16, ann('f11_floatvector', '[0.8882545, 0.628688, 0.8879052, 0.11532456, 0.4503705, 0.9680428, 0.843306, 0.7594733, 0.9998952, 0.3091762, 0.5800451, 0.47775137, 0.14322859, 0.64815366, 0.53906167, 0.67647696, 0.42037117, 0.059540927, 0.33235663, 0.19300634, 0.35207045, 0.40025198, 0.30008793, 0.4697852, 0.8702338, 0.24930298, 0.5665423, 0.637488, 0.0972411, 0.53321683, 0.5086088, 0.57623714, 0.7873297, 0.2055744, 0.7391355, 0.49468058, 0.77539134, 0.986524, 0.43884748, 0.018261313, 0.6838007, 0.56789255, 0.4343794, 0.3246022, 0.8703792, 0.34895402, 0.49710017, 0.1979323, 0.13002282, 0.88709956, 0.24906135, 0.5164073, 0.22423118, 0.9799434, 0.020503461, 0.2346574, 0.657751, 0.9972303, 0.3604462, 0.30569005, 0.7663866, 0.5618555, 0.401128, 0.70626426, 0.65696263, 0.4520629, 0.30224097, 0.12104744, 0.6473011, 0.12727118, 0.82644415, 0.31439477, 0.9043705, 0.23693651, 0.13856292, 0.5137155, 0.051585674, 0.97746503, 0.98287, 0.5624919, 0.7063009, 0.041630268, 0.92217296, 0.31960183, 0.57621056, 0.9365577, 0.6560259, 0.008300364, 0.7646108, 0.6115488, 0.050772727, 0.50479466, 0.9509033, 0.07040185, 0.27948397, 0.032683313, 0.7815432, 0.08214998, 0.8939561, 0.3536362, 0.050723255, 0.34008121, 0.13321483, 0.3526914, 0.5078966, 0.74549913, 0.8534424, 0.272707, 0.60489553, 0.6569729, 0.07359815, 0.6992027, 0.40861297, 0.3980273, 0.8405081, 0.50457394, 0.74133945, 0.45964152, 0.3887775, 0.17808491, 0.37086087, 0.27501774, 0.007075429, 0.4719149, 0.52292705, 0.24240643, 0.61630404, 0.83871365]') as __score__ from milvus.milvus_table_1 where f2_int8<93 and f3_int16>0 order by __score__ limit 11";
        // String sql = "select f4_int32, f3_int16, ann('f11_floatvector', '[0.8882545, 0.628688, 0.8879052, 0.11532456, 0.4503705, 0.9680428, 0.843306, 0.7594733, 0.9998952, 0.3091762, 0.5800451, 0.47775137, 0.14322859, 0.64815366, 0.53906167, 0.67647696, 0.42037117, 0.059540927, 0.33235663, 0.19300634, 0.35207045, 0.40025198, 0.30008793, 0.4697852, 0.8702338, 0.24930298, 0.5665423, 0.637488, 0.0972411, 0.53321683, 0.5086088, 0.57623714, 0.7873297, 0.2055744, 0.7391355, 0.49468058, 0.77539134, 0.986524, 0.43884748, 0.018261313, 0.6838007, 0.56789255, 0.4343794, 0.3246022, 0.8703792, 0.34895402, 0.49710017, 0.1979323, 0.13002282, 0.88709956, 0.24906135, 0.5164073, 0.22423118, 0.9799434, 0.020503461, 0.2346574, 0.657751, 0.9972303, 0.3604462, 0.30569005, 0.7663866, 0.5618555, 0.401128, 0.70626426, 0.65696263, 0.4520629, 0.30224097, 0.12104744, 0.6473011, 0.12727118, 0.82644415, 0.31439477, 0.9043705, 0.23693651, 0.13856292, 0.5137155, 0.051585674, 0.97746503, 0.98287, 0.5624919, 0.7063009, 0.041630268, 0.92217296, 0.31960183, 0.57621056, 0.9365577, 0.6560259, 0.008300364, 0.7646108, 0.6115488, 0.050772727, 0.50479466, 0.9509033, 0.07040185, 0.27948397, 0.032683313, 0.7815432, 0.08214998, 0.8939561, 0.3536362, 0.050723255, 0.34008121, 0.13321483, 0.3526914, 0.5078966, 0.74549913, 0.8534424, 0.272707, 0.60489553, 0.6569729, 0.07359815, 0.6992027, 0.40861297, 0.3980273, 0.8405081, 0.50457394, 0.74133945, 0.45964152, 0.3887775, 0.17808491, 0.37086087, 0.27501774, 0.007075429, 0.4719149, 0.52292705, 0.24240643, 0.61630404, 0.83871365]') from milvus.milvus_table_1 where f2_int8<93 and f3_int16>0 order by ann('f11_floatvector', '[0.8882545, 0.628688, 0.8879052, 0.11532456, 0.4503705, 0.9680428, 0.843306, 0.7594733, 0.9998952, 0.3091762, 0.5800451, 0.47775137, 0.14322859, 0.64815366, 0.53906167, 0.67647696, 0.42037117, 0.059540927, 0.33235663, 0.19300634, 0.35207045, 0.40025198, 0.30008793, 0.4697852, 0.8702338, 0.24930298, 0.5665423, 0.637488, 0.0972411, 0.53321683, 0.5086088, 0.57623714, 0.7873297, 0.2055744, 0.7391355, 0.49468058, 0.77539134, 0.986524, 0.43884748, 0.018261313, 0.6838007, 0.56789255, 0.4343794, 0.3246022, 0.8703792, 0.34895402, 0.49710017, 0.1979323, 0.13002282, 0.88709956, 0.24906135, 0.5164073, 0.22423118, 0.9799434, 0.020503461, 0.2346574, 0.657751, 0.9972303, 0.3604462, 0.30569005, 0.7663866, 0.5618555, 0.401128, 0.70626426, 0.65696263, 0.4520629, 0.30224097, 0.12104744, 0.6473011, 0.12727118, 0.82644415, 0.31439477, 0.9043705, 0.23693651, 0.13856292, 0.5137155, 0.051585674, 0.97746503, 0.98287, 0.5624919, 0.7063009, 0.041630268, 0.92217296, 0.31960183, 0.57621056, 0.9365577, 0.6560259, 0.008300364, 0.7646108, 0.6115488, 0.050772727, 0.50479466, 0.9509033, 0.07040185, 0.27948397, 0.032683313, 0.7815432, 0.08214998, 0.8939561, 0.3536362, 0.050723255, 0.34008121, 0.13321483, 0.3526914, 0.5078966, 0.74549913, 0.8534424, 0.272707, 0.60489553, 0.6569729, 0.07359815, 0.6992027, 0.40861297, 0.3980273, 0.8405081, 0.50457394, 0.74133945, 0.45964152, 0.3887775, 0.17808491, 0.37086087, 0.27501774, 0.007075429, 0.4719149, 0.52292705, 0.24240643, 0.61630404, 0.83871365]') limit 11";

        String sql = "select id, f4_int32, f3_int16, ann('f11_floatvector', '[0.8882545, 0.628688, 0.8879052, 0.11532456, 0.4503705, 0.9680428, 0.843306, 0.7594733, 0.9998952, 0.3091762, 0.5800451, 0.47775137, 0.14322859, 0.64815366, 0.53906167, 0.67647696, 0.42037117, 0.059540927, 0.33235663, 0.19300634, 0.35207045, 0.40025198, 0.30008793, 0.4697852, 0.8702338, 0.24930298, 0.5665423, 0.637488, 0.0972411, 0.53321683, 0.5086088, 0.57623714, 0.7873297, 0.2055744, 0.7391355, 0.49468058, 0.77539134, 0.986524, 0.43884748, 0.018261313, 0.6838007, 0.56789255, 0.4343794, 0.3246022, 0.8703792, 0.34895402, 0.49710017, 0.1979323, 0.13002282, 0.88709956, 0.24906135, 0.5164073, 0.22423118, 0.9799434, 0.020503461, 0.2346574, 0.657751, 0.9972303, 0.3604462, 0.30569005, 0.7663866, 0.5618555, 0.401128, 0.70626426, 0.65696263, 0.4520629, 0.30224097, 0.12104744, 0.6473011, 0.12727118, 0.82644415, 0.31439477, 0.9043705, 0.23693651, 0.13856292, 0.5137155, 0.051585674, 0.97746503, 0.98287, 0.5624919, 0.7063009, 0.041630268, 0.92217296, 0.31960183, 0.57621056, 0.9365577, 0.6560259, 0.008300364, 0.7646108, 0.6115488, 0.050772727, 0.50479466, 0.9509033, 0.07040185, 0.27948397, 0.032683313, 0.7815432, 0.08214998, 0.8939561, 0.3536362, 0.050723255, 0.34008121, 0.13321483, 0.3526914, 0.5078966, 0.74549913, 0.8534424, 0.272707, 0.60489553, 0.6569729, 0.07359815, 0.6992027, 0.40861297, 0.3980273, 0.8405081, 0.50457394, 0.74133945, 0.45964152, 0.3887775, 0.17808491, 0.37086087, 0.27501774, 0.007075429, 0.4719149, 0.52292705, 0.24240643, 0.61630404, 0.83871365]') as dist from milvus.milvus_table_1 order by dist desc limit 15";


        System.out.println("初始计划===================");
        printPlan(connection, sql);
        Statement statement0 = connection.createStatement();
        ResultSet resultSet0 = statement0.executeQuery("explain plan for " + sql);
        System.out.println("优化后的计划===================");
        printResultSet(resultSet0);


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

    public static void printPlan(Connection calciteConnection, String sql) throws Exception {
        calciteConnection = calciteConnection.unwrap(CalciteConnection.class);
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
