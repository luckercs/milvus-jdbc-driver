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
import java.util.Properties;

/**
 * 支持 milvus 数据源 和 其他 JDBC数据源混合查询
 */
public class MilvusJdbcDemo2 {

    private static final Logger LOG = LoggerFactory.getLogger(MilvusJdbcDemo2.class);

    private static final String MILVUS_JDBC_DRIVER = "com.milvus.jdbc.Driver";
    private static final String MILVUS_JDBC_URL = "jdbc:milvus:";

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("milvus.url", "http://localhost:19530");
        properties.setProperty("milvus.user", "root");
        properties.setProperty("milvus.password", "milvus");
        properties.setProperty("milvus.db", "default");

        properties.setProperty("jdbc.driver", "com.mysql.cj.jdbc.Driver");
        properties.setProperty("jdbc.url", "jdbc:mysql://localhost:3306/db1");
        properties.setProperty("jdbc.user", "root");
        properties.setProperty("jdbc.password", "mysql");
        Class.forName(MILVUS_JDBC_DRIVER);
        Connection connection = DriverManager.getConnection(MILVUS_JDBC_URL, properties);

        String sql = "SELECT id, name, age FROM mytable";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
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

    public static void printPlan( CalciteConnection calciteConnection,  String sql) throws Exception {
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
