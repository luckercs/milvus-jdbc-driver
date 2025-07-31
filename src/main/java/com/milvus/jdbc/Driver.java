package com.milvus.jdbc;

import com.milvus.connector.MilvusSchemaOptions;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class Driver extends org.apache.calcite.jdbc.Driver {

    public static final String CONNECT_STRING_PREFIX = "jdbc:milvus:";
    private static final String DATASOURCE_MILVUS = "schema.milvus.";
    private static final String DATASOURCE_JDBC = "schema.jdbc.";

    static {
        (new Driver()).register();
    }

    public String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    // driver properties 往 schemafactory 传递参数，参数前缀必须是schema., 传递到 schemafactory 后，会去掉schema.前缀
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            throw new SQLException("URL " + url + " not supported");
        }

        info.setProperty("schema.name", "milvus");
        info.setProperty("schema.type", "custom");
        info.setProperty("schemaFactory", "com.milvus.connector.MilvusSchemaFactory");
        info.setProperty("defaultSchema", "milvus");

        if (!info.containsKey(DATASOURCE_MILVUS + MilvusSchemaOptions.UserName)) {
            info.setProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.UserName, info.getProperty("user", "root"));
        }
        if (!info.containsKey(DATASOURCE_MILVUS + MilvusSchemaOptions.PassWord)) {
            info.setProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.PassWord, info.getProperty("password", ""));
        }

        if (url.contains("//")) {
            parseMilvusUrl(url, info);
        }

        return super.connect(url, info);
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
            props.setProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.URL, url_suffixes);
        } else {
            props.setProperty(DATASOURCE_MILVUS + MilvusSchemaOptions.URL, url_suffixes.split("/")[0]);
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
    }
}
