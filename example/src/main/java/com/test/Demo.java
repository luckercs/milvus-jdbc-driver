package com.test;

import com.milvus.options.MilvusSchemaOptions;

import java.util.Properties;

public class Demo {
    public static void main(String[] args) {
        String url1 = "jdbc:milvus://localhost:19530/test?useSSL=false&key2=value2";
        String url2 = "jdbc:milvus://localhost:19530/test?useSSL=false";
        String url3 = "jdbc:milvus://localhost:19530/test";
        String url4 = "jdbc:milvus://localhost:19530/";
        String url5 = "jdbc:milvus://localhost:19530";

        System.out.println("========");
        System.out.println(url1);
        Properties properties = parseUrl(url1);
        System.out.println(properties);
        System.out.println("========");

        System.out.println(url2);
        properties = parseUrl(url2);
        System.out.println(properties);
        System.out.println("========");

        System.out.println(url3);
        properties = parseUrl(url3);
        System.out.println(properties);
        System.out.println("========");

        System.out.println(url4);
        properties = parseUrl(url4);
        System.out.println(properties);
        System.out.println("========");

        System.out.println(url5);
        properties = parseUrl(url5);
        System.out.println(properties);
        System.out.println("========");
    }


    private static Properties parseUrl(String url) {
        Properties props = new Properties();
        props.setProperty(MilvusSchemaOptions.DB, "default");
        props.setProperty(MilvusSchemaOptions.UseSSL, "false");

        String url_suffixes = url.split("//")[1];
        if (!url_suffixes.contains("/")) {
            props.setProperty(MilvusSchemaOptions.URL, url_suffixes);
        } else {
            props.setProperty(MilvusSchemaOptions.URL, url_suffixes.split("/")[0]);
            if (url_suffixes.split("/").length > 1) {
                String db_suffixes = url_suffixes.split("/")[1];
                if (!db_suffixes.contains("?")) {
                    props.setProperty(MilvusSchemaOptions.DB, db_suffixes);
                } else {
                    props.setProperty(MilvusSchemaOptions.DB, db_suffixes.split("\\?")[0]);
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
