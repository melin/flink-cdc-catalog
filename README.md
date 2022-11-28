### Flink CDC Catalog


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
1. [Mac M1 Docker 安装 Oracle](https://www.dbasolved.com/2022/09/running-x86_64-docker-images-on-mac-m1-max-oracle-database-19c/)
2. [Viewing Tables Accessible by Current User](https://chartio.com/resources/tutorials/how-to-list-all-tables-in-oracle/#viewing-tables-accessible-by-current-user)
