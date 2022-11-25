package com.superior.flink.jdbc;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SqlServerJdbcCatalogExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        String sql = "CREATE CATALOG sqlserver_catalog WITH(\n" +
                "    'type' = 'jdbc_sqlserver',\n" +
                "    'base-url' = 'jdbc:mysql://10.0.8.2:3306',\n" +
                "    'default-database' = 'example',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'mysql2022#'\n" +
                ");";
        tableEnvironment.executeSql(sql);
        tableEnvironment.useCatalog("mysql_cdc_catalog");

        tableEnvironment.executeSql("SELECT * FROM user_city /*+ OPTIONS('scan.startup.mode'='latest-offset') */").print();
    }
}
