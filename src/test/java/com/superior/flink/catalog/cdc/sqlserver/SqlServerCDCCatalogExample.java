package com.superior.flink.catalog.cdc.sqlserver;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SqlServerCDCCatalogExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        String sql = "CREATE CATALOG sqlserver_catalog WITH(\n" +
                "    'type' = 'flink_cdc',\n" +
                "    'base-url' = 'jdbc:sqlserver://localhost:1433',\n" +
                "    'default-database' = 'inventory',\n" +
                "    'schema-name' = 'dbo',\n" +
                "    'username' = 'sa',\n" +
                "    'password' = 'Sqlserver@2022'\n" +
                ");";
        tableEnvironment.executeSql(sql);
        tableEnvironment.useCatalog("sqlserver_catalog");

        tableEnvironment.executeSql("SELECT name, SUM(weight) FROM dbo.products GROUP BY name").print();
    }
}
