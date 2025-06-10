package test;

import com.connector.FeatureGen;
import com.connector.MilvusSchemaFactory;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.schema.impl.TableFunctionImpl;
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
import java.util.*;

/**
 * @author renjianting001
 * @since 2023年03月02日 14:38:35
 */
public class CalciteMilvusTestDemo {
    private static final Logger LOG = LoggerFactory.getLogger(CalciteMilvusTestDemo.class);
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("caseSensitive", "false");
//        info.setProperty("parserFactory", "com.parser.gen.MilvusSqlParserImpl#FACTORY");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        HashMap<String, Object> milvusOperand = new HashMap<>();
        milvusOperand.put("milvusConnectURL","http://localhost:19530");
        milvusOperand.put("milvusConnectUsername","root");
        milvusOperand.put("milvusConnectPassword","1qaz346,.");
        milvusOperand.put("milvusConnectDatabase","test");
        Schema milvusSchema = new MilvusSchemaFactory().create(rootSchema, "milvus", milvusOperand);
        rootSchema.add("milvus", milvusSchema);

        rootSchema.add("gen_vector", ScalarFunctionImpl.create(FeatureGen.class, "gen_random_float_vectors_str"));

        List<List<Float>> vectorList = new ArrayList<>();
        Random random = new Random();
        ArrayList<Float> vec = new ArrayList<>();
        for (int k = 0; k < 128; k++) {
            vec.add(random.nextFloat());
        }
        vectorList.add(vec);
        String vecStr = vectorList.toString();

        Statement statement = calciteConnection.createStatement();
        String sql = "select * from milvus.test_tbl limit 7";
        String sql2 = "select f3,f4,f5,id from milvus.test_tbl limit 101";
        String sql3 = "select count(*) from milvus.test_tbl";
        String sql4 = "select t1.id,t2.id from milvus.test_tbl as t1 join milvus.test_tbl2 as t2 on t1.id=t2.id";
        String sql5 = String.format("select * from table(milvus_search('test','test_tbl',null,'id,vec','vec',10,null,'L2','{\"nprobe\":10,\"offset\":0 }','%s',false)) as t", vecStr);

        // 语法修改
        String sql6 = "select * from milvus.test_tbl where vec like [[]] and  milvus_search_params(params)";
        String sql8 = "select * from milvus.test_tbl where vec like genVec(128,2) and milvus_search(params)";

        // hints spark hints
        String sql10 = "select * from milvus.test_tbl where vec like genVec(128,2) and milvus_search(params)";
        String sql9 = "select * from milvus.test_tbl where vec like genVec(128,2) and milvus_search(params)";
        String sql7 = "select * from milvus.test_tbl where vec in (select genvec(pic,128) from tbl where id >5) and milvus_search(params)";

        // rule
        String sql11 = "select * from milvus.test_tbl where vec similarto '[[0.1,0.2,0.3]]' with metric 'IP' with params '{}' and pt='xx' limit 10";
//        select * from tbl where vec vlike '[[]]' with metric IP [with params '{}'] [with partitions 'str'] [with expr 'id > 3'] limit x;
//        select * from tbl where vec = [] and __metric = 'xxx' and __expr='id>3' limit x;
//        select * from tbl where vec = [] and query('x','x','y','x') limit x;


        String sql12 = String.format("select id,vec,f6 from milvus.test_tbl where id < '%s' and f7>678 and id like '79' limit 10",vecStr);
        /*
         *  LogicalSort(fetch=[10])
              LogicalProject(id=[$0], vec=[$1], f3=[$2], f4=[$3], f5=[$4], f6=[$5], f7=[$6], f8=[$7], f9=[$8], f10=[$9], f11=[$10])
                LogicalFilter(condition=[=($0, '[[0.94632673, 0.35059988, 0.70491815, 0.2882583, 0.03800565, 0.020186722, 0.18535018, 0.76453364, 0.7929383, 0.3904841, 0.4579029, 0.5027294, 0.703032, 0.17673504, 0.5589551, 0.65658826, 0.6719368, 0.5508689, 0.6201357, 0.33640093, 0.5031363, 0.26788843, 0.35538983, 0.41285515, 0.014171243, 0.077953815, 0.28491497, 0.92261165, 0.7555609, 0.70925874, 0.07290524, 0.35244828, 0.3693881, 0.70403415, 0.66283506, 0.7286743, 0.23659265, 0.26793557, 0.4132775, 0.09063506, 0.89099973, 0.18935698, 0.6777624, 0.5395122, 0.6448517, 0.890699, 0.63336134, 0.6624094, 0.4135819, 0.08223587, 0.18456942, 0.10115111, 0.050878465, 0.04805243, 0.38230902, 0.30770618, 0.82845217, 0.7527532, 0.573795, 0.8741709, 0.5037043, 0.120595634, 0.89948493, 0.09345788, 0.71365964, 0.6118227, 0.38093662, 0.7739787, 0.9816915, 0.6638283, 0.6912705, 0.55257857, 0.20694077, 0.44008273, 0.40186918, 0.6077723, 0.17278165, 0.35329473, 0.5793685, 0.431091, 0.92367464, 0.92003304, 0.7309268, 0.15732121, 0.1483903, 0.034049153, 0.3188789, 0.45199805, 0.31659627, 0.7900258, 0.08446908, 0.5022009, 0.8555408, 0.5767444, 0.82133305, 0.72812974, 0.2500038, 0.5201652, 0.7502953, 0.22987211, 0.49342847, 0.17721385, 0.38603777, 0.086082935, 0.71842146, 0.047866285, 0.5120582, 0.63700694, 0.26345974, 0.3272391, 0.24249315, 0.09950024, 0.5781571, 0.90476775, 0.9378678, 0.8907192, 0.68591255, 0.9312653, 0.13731402, 0.07232082, 0.87992, 0.016891181, 0.4935652, 0.38886553, 0.66171485, 0.9471455, 0.6609971, 0.56126595]]')])
                  MilvusTableScan(table=[[milvus, test_tbl]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]])
         */

        String sql13 = String.format("select  id, vec,f6 from milvus.test_tbl where  id < '%s' limit 10",vecStr);
        String sql14 = String.format("select id, vec,f6 from milvus.test_tbl where id < '%s' limit 10",vecStr);

        String sql15 = String.format("select id,f6,vec from milvus.test_tbl where __search_vector_field='vec' " +
                "and __search_vector='%s' and __search_metric_type='L2' and __search_params='{\"nprobe\":10,\"offset\":0 }' limit 9", vecStr);

        String sql16 = "select gen_vector(128,1) as f1";
        String sql17 = "select id,f6,vec from milvus.test_tbl where __search_vector_field='vec' and __search_vector=gen_vector(128,1) and __search_metric_type='L2' and __search_params='{\"nprobe\":10,\"offset\":0 }'  limit 9";
        String sql18 = "select id,f6,vec from milvus.test_tbl where 1=1 limit 9";


        String execSQL = sql16;
//        System.out.println(execSQL);
//        printPlan(calciteConnection, execSQL);

        ResultSet resultSet = statement.executeQuery(execSQL);
        printResultSet(resultSet);

        resultSet.close();
        statement.close();
        connection.close();
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
