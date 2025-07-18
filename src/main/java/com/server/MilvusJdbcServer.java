package com.server;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteMetaImpl;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.util.List;

public class MilvusJdbcServer {


    public static void main(String[] args) throws InterruptedException, ClassNotFoundException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException {

        // "org.apache.calcite.avatica.Meta.Factory"
        org.apache.calcite.avatica.server.Main.main(args);
    }
}


