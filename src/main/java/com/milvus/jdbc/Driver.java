package com.milvus.jdbc;

import com.milvus.functions.AnnFunction;
import com.milvus.functions.AnnFunctionImpl;
import com.milvus.functions.FeatureGen;
import com.milvus.connector.MilvusSchema;
import com.milvus.options.MilvusSchemaOptions;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteJdbc41Factory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

public class Driver extends org.apache.calcite.jdbc.Driver {

    public static final String CONNECT_STRING_PREFIX = "jdbc:milvus:";

    private static final String DATASOURCE_MILVUS = "milvus.";
    private static final String DATASOURCE_JDBC = "jdbc.";

    static {
        (new Driver()).register();
    }

    public String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            throw new SQLException("URL " + url + " not supported");
        }
        info.setProperty("lex", "MYSQL");
        Connection connection = super.connect(url, info);

        if (!info.containsKey(DATASOURCE_MILVUS + MilvusSchemaOptions.User)) {
            info.setProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.User, info.getProperty("user", "root"));
        }
        if (!info.containsKey(DATASOURCE_MILVUS + MilvusSchemaOptions.PassWord)) {
            info.setProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.PassWord, info.getProperty("password", ""));
        }
        if (url.contains("//")) {
            parseMilvusUrl(url, info);
        }

        // add schemas
//        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        Properties milvusProps = filterDataSourceProps(info, DATASOURCE_MILVUS);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("milvus", new MilvusSchema(
                        milvusProps.getProperty(MilvusSchemaOptions.URI, MilvusSchemaOptions.getStringDefaultValue(MilvusSchemaOptions.URI)).trim(),
                        milvusProps.getProperty(MilvusSchemaOptions.User, MilvusSchemaOptions.getStringDefaultValue(MilvusSchemaOptions.User)).trim(),
                        milvusProps.getProperty(MilvusSchemaOptions.PassWord, MilvusSchemaOptions.getStringDefaultValue(MilvusSchemaOptions.PassWord)).trim(),
                        milvusProps.getProperty(MilvusSchemaOptions.DB, MilvusSchemaOptions.getStringDefaultValue(MilvusSchemaOptions.DB)).trim(),
                        Integer.parseInt(milvusProps.getProperty(MilvusSchemaOptions.TimeOutMs, MilvusSchemaOptions.getIntDefaultValue(MilvusSchemaOptions.TimeOutMs).toString()).trim()),
                        Boolean.parseBoolean(milvusProps.getProperty(MilvusSchemaOptions.UseSSL, MilvusSchemaOptions.getBoolDefaultValue(MilvusSchemaOptions.UseSSL).toString()).trim()),
                        Integer.parseInt(milvusProps.getProperty(MilvusSchemaOptions.BatchSize, MilvusSchemaOptions.getIntDefaultValue(MilvusSchemaOptions.BatchSize).toString()).trim())
                )
        );

//        // add functions
//        ListSqlOperatorTable customFunctions = new ListSqlOperatorTable();
//        customFunctions.add(new AnnFunction());
//        ChainedSqlOperatorTable operatorTable = new ChainedSqlOperatorTable(Arrays.asList(
//                SqlStdOperatorTable.instance(),
//                customFunctions
//        ));

//        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder().defaultSchema(rootSchema).operatorTable(operatorTable).build();
//        info.put(FrameworkConfig.class.getName(), frameworkConfig);
//        CalciteJdbc41Factory factory = new CalciteJdbc41Factory();
//        AvaticaConnection avaticaConnection = factory.newConnection(this, factory, url, info);
//        this.handler.onConnectionInit(avaticaConnection);
//        CalciteConnection calciteConnection = avaticaConnection.unwrap(CalciteConnection.class);

        rootSchema.add("gen_vector", ScalarFunctionImpl.create(FeatureGen.class, "gen_random_float_vectors_str"));
        rootSchema.add("ann", ScalarFunctionImpl.create(AnnFunctionImpl.class, "processFloatArrays"));
        return calciteConnection;
    }

    private void parseMilvusUrl(String url, Properties props) {
        if (!props.containsKey(DATASOURCE_MILVUS + MilvusSchemaOptions.UseSSL)) {
            props.setProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.UseSSL, "false");
        }
        if (!props.containsKey(DATASOURCE_MILVUS + MilvusSchemaOptions.DB)) {
            props.setProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.DB, "default");
        }

        String url_suffixes = url.split("//")[1];
        if (!url_suffixes.contains("/")) {
            props.setProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.URI, url_suffixes);
        } else {
            props.setProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.URI, url_suffixes.split("/")[0]);
            if (url_suffixes.split("/").length > 1) {
                String db_suffixes = url_suffixes.split("/")[1];
                if (!db_suffixes.contains("?")) {
                    props.setProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.DB, db_suffixes);
                } else {
                    props.setProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.DB, db_suffixes.split("\\?")[0]);
                    if (db_suffixes.split("\\?").length > 1) {
                        String properties_str = db_suffixes.split("\\?")[1];
                        String[] properties_split = properties_str.split("&");
                        for (String property_kv : properties_split) {
                            props.setProperty(DATASOURCE_MILVUS + property_kv.split("=")[0], property_kv.split("=")[1]);
                        }
                    }
                }
            }
        }
        String uriWithoutPrefix = props.getProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.URI);
        if (props.getProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.UseSSL).equals("true")) {
            props.setProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.URI, "https://" + uriWithoutPrefix);
        } else {
            props.setProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.URI, "http://" + uriWithoutPrefix);
        }
    }

    private static Properties filterDataSourceProps(Properties originalProperties, String dataSourceName) {
        Properties resProperties = new Properties();

        if (originalProperties == null) {
            return resProperties;
        }

        Set<Object> keys = originalProperties.keySet();
        for (Object key : keys) {
            if (key != null && ((String) key).startsWith(dataSourceName)) {
                String value = (String) originalProperties.get(key);
                resProperties.setProperty(((String) key).replace(dataSourceName, ""), value);
            }
        }
        return resProperties;
    }
}
