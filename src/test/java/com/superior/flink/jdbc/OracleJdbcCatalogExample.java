package com.superior.flink.jdbc;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OracleJdbcCatalogExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        String sql = "CREATE CATALOG oracle_catalog WITH(\n" +
                "    'type' = 'jdbc_oracle',\n" +
                "    'base-url' = 'jdbc:oracle:thin:@//172.18.1.56:1521',\n" +
                "    'default-database' = 'XE',\n" +
                "    'username' = 'flinkuser',\n" +
                "    'password' = 'flinkpw'\n" +
                ");";
        tableEnvironment.executeSql(sql);
        tableEnvironment.useCatalog("oracle_catalog");

        tableEnvironment.executeSql("SELECT * FROM oracle_catalog.FLINKUSER.ORDERS").print();
    }
}
