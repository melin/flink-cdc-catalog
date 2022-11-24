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

SELECT * FROM user_city /*+ OPTIONS('scan.startup.mode'='latest-offset') */
```
