package com.examples.flink.cdc;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MySqlCDCExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        String sql = "CREATE TABLE mysql_binlog (\n" +
                "                id INT NOT NULL,\n" +
                "                username STRING,\n" +
                "                city STRING,\n" +
                "                PRIMARY KEY (id) NOT ENFORCED\n" +
                "        ) WITH (\n" +
                "                'connector' = 'mysql-cdc',\n" +
                "                'hostname' = '10.0.8.2',\n" +
                "                'port' = '3306',\n" +
                "                'username' = 'root',\n" +
                "                'password' = 'mysql2022#',\n" +
                "                'database-name' = 'example',\n" +
                "                'table-name' = 'user_city'\n" +
                "        )";
        tableEnvironment.executeSql(sql);

        tableEnvironment.executeSql("SELECT * FROM mysql_binlog").print();
    }
}
