package com.superior.flink.catalog.cdc;

import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;

import java.net.URI;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.DATABASE_NAME;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.SCHEMA_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

abstract public class AbstractJdbcCatalog extends org.apache.flink.connector.jdbc.catalog.AbstractJdbcCatalog {

    protected final String defaultDatabase;

    public AbstractJdbcCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        super(userClassLoader, catalogName, defaultDatabase, username, pwd, baseUrl);
        this.defaultDatabase = defaultDatabase;
    }

    abstract protected String getJdbcUrl();

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<UniqueConstraint> primaryKey =
                    getPrimaryKey(
                            metaData,
                            defaultDatabase,
                            getSchemaName(tablePath),
                            getTableName(tablePath));

            PreparedStatement ps =
                    conn.prepareStatement(
                            String.format("SELECT * FROM %s;", getSchemaTableName(tablePath)));

            ResultSetMetaData resultSetMetaData = ps.getMetaData();

            String[] columnNames = new String[resultSetMetaData.getColumnCount()];
            DataType[] types = new DataType[resultSetMetaData.getColumnCount()];

            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                columnNames[i - 1] = resultSetMetaData.getColumnName(i);
                types[i - 1] = fromJDBCType(tablePath, resultSetMetaData, i);
                if (resultSetMetaData.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                    types[i - 1] = types[i - 1].notNull();
                }
            }

            Schema.Builder schemaBuilder = Schema.newBuilder().fromFields(columnNames, types);
            primaryKey.ifPresent(
                    pk -> schemaBuilder.primaryKeyNamed(pk.getName(), pk.getColumns()));
            Schema tableSchema = schemaBuilder.build();

            Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR.key(), getConnector());
            props.put(USERNAME.key(), username);
            props.put(PASSWORD.key(), pwd);

            String cleanURI = baseUrl.substring(5);
            URI uri = URI.create(cleanURI);
            props.put(MySqlSourceOptions.HOSTNAME.key(), uri.getHost());
            props.put(MySqlSourceOptions.PORT.key(), String.valueOf(uri.getPort()));
            props.put(DATABASE_NAME.key(), getDefaultDatabase());
            if (!getDefaultDatabase().equals(getSchemaName(tablePath))) {
                props.put(SCHEMA_NAME.key(), getSchemaName(tablePath));
            }
            props.put(TABLE_NAME.key(), getSchemaTableName(tablePath));

            return CatalogTable.of(tableSchema, null, Lists.newArrayList(), props);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    abstract protected String getConnector();
}
