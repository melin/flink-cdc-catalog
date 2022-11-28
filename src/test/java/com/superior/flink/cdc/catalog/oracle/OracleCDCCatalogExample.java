package com.superior.flink.cdc.catalog.oracle;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OracleCDCCatalogExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        String sql = "CREATE CATALOG oracle_catalog WITH(\n" +
                "    'type' = 'flink_cdc',\n" +
                "    'base-url' = 'jdbc:oracle:thin:@//localhost:1521',\n" +
                "    'default-database' = 'XE',\n" +
                "    'schema-name' = 'FLINKUSER',\n" +
                "    'username' = 'flinkuser',\n" +
                "    'password' = 'flinkpw'\n" +
                ");";
        tableEnvironment.executeSql(sql);
        tableEnvironment.useCatalog("oracle_catalog");

        tableEnvironment.executeSql("SELECT * FROM FLINKUSER.TEST1 " +
                "/*+ options('scan.incremental.snapshot.chunk.size' = '2', " +
                "'debezium.log.mining.strategy' = 'online_catalog', 'debezium.log.mining.continuous.mine' = 'true') */").print();
    }
}
