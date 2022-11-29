### Flink CDC Catalog

目前已经支持 MySQL、Postgres、Oracle Catalog


### Example
```SQL
-- 注册 Catalog
CREATE CATALOG mysql_cdc_catalog WITH(
    'type' = 'flink_cdc',
    'base-url' = 'jdbc:mysql://10.0.8.2:3306',
    'default-database' = 'example',
    'username' = 'root',
    'password' = 'mysql2022#'
);

SELECT * FROM mysql_cdc_catalog.example.user_city /*+ OPTIONS('scan.startup.mode'='latest-offset') */
```


### 参考
1. [基于 Flink CDC 实现 Oracle 数据实时更新到 Kudu](https://cloud.tencent.com/developer/article/1949088)
2. [Viewing Tables Accessible by Current User](https://chartio.com/resources/tutorials/how-to-list-all-tables-in-oracle/#viewing-tables-accessible-by-current-user)
