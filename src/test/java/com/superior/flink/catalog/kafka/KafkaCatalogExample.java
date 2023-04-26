package com.superior.flink.catalog.kafka;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaCatalogExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        String sql = "CREATE CATALOG kafka_catalog WITH(\n" +
                "    'type' = 'kafka',\n" +
                "    'properties.bootstrap.servers' = '172.18.5.44:9092'\n" +
                ");";
        tableEnvironment.executeSql(sql);
        tableEnvironment.useCatalog("kafka_catalog");

        tableEnvironment.executeSql("SELECT * FROM kafka_catalog.kafka.`quickstart-events` " +
                "/*+ OPTIONS('properties.group.id' = 'kafka_demo', 'format' = 'json', 'scan.startup.mode' = 'earliest-offset') */").print();
    }
}
