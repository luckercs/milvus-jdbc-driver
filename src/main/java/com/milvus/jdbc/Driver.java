package com.milvus.jdbc;

import com.milvus.connector.MilvusOptions;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class Driver extends org.apache.calcite.jdbc.Driver {

    public static final String CONNECT_STRING_PREFIX = "jdbc:milvus:";

    static {
        new Driver().register();
    }

    public String getConnectStringPrefix(){
        return CONNECT_STRING_PREFIX;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            throw new SQLException("URL " + url + " not supported");
        }

        info.setProperty("defaultSchema", "milvus");
        info.setProperty("schema.milvus.type", "custom");
        info.setProperty("schema.milvus.factory", "com.milvus.connector.MilvusSchemaFactory");
        info.setProperty("schema.milvus.encoding", "UTF-8");

        Properties properties = parseUrl(url);
        info.putAll(properties);

        return super.connect(url, info);
    }

    private Properties parseUrl(String url) {
        Properties props = new Properties();
        props.setProperty(MilvusOptions.DB, "default");
        props.setProperty(MilvusOptions.UseSSL, "false");

        String url_suffixes = url.split("//")[1];
        if (!url_suffixes.contains("/")) {
            props.setProperty(MilvusOptions.URL, url_suffixes);
        } else {
            props.setProperty(MilvusOptions.URL, url_suffixes.split("/")[0]);
            if (url_suffixes.split("/").length > 1) {
                String db_suffixes = url_suffixes.split("/")[1];
                if (!db_suffixes.contains("?")) {
                    props.setProperty(MilvusOptions.DB, db_suffixes);
                } else {
                    props.setProperty(MilvusOptions.DB, db_suffixes.split("\\?")[0]);
                    if (db_suffixes.split("\\?").length > 1) {
                        String properties_str = db_suffixes.split("\\?")[1];
                        String[] properties_split = properties_str.split("&");
                        for (String property_kv : properties_split) {
                            props.setProperty(property_kv.split("=")[0], property_kv.split("=")[1]);
                        }
                    }
                }
            }
        }
        return props;
    }
}
