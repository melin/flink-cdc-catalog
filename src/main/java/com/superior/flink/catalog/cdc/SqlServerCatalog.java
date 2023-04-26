package com.superior.flink.catalog.cdc;

import com.superior.flink.catalog.jdbc.mapper.SqlServerTypeMapper;
import com.ververica.cdc.connectors.sqlserver.table.SqlServerTableFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectTypeMapper;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlServerCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(SqlServerCatalog.class);

    private static final String SQLSERVER_CONNECTOR = "sqlserver-cdc";

    private final JdbcDialectTypeMapper dialectTypeMapper;

    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("INFORMATION_SCHEMA");
                    add("guest");
                    add("sys");
                    add("cdc");
                    add("db_owner");
                    add("db_accessadmin");
                    add("db_securityadmin");
                    add("db_ddladmin");
                    add("db_backupoperator");
                    add("db_datareader");
                    add("db_datawriter");
                    add("db_denydatareader");
                    add("db_denydatawriter");
                }
            };

    public SqlServerCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String schemaName,
            String username,
            String pwd,
            String baseUrl) {
        super(userClassLoader, catalogName, defaultDatabase, username, pwd, baseUrl);

        String driverVersion =
                Preconditions.checkNotNull(getDriverVersion(), "Driver version must not be null.");
        String databaseVersion =
                Preconditions.checkNotNull(
                        getDatabaseVersion(), "Database version must not be null.");
        LOG.info("Driver version: {}, database version: {}", driverVersion, databaseVersion);
        this.dialectTypeMapper = new SqlServerTypeMapper(databaseVersion, driverVersion);
    }

    @Override
    protected String getJdbcUrl() {
        return StringUtils.substringBeforeLast(this.baseUrl, "/") + ";DatabaseName=" + defaultDatabase;
    }

    @Override
    public void open() throws CatalogException {
        // load the Driver use userClassLoader explicitly, see FLINK-15635 for more detail
        try (TemporaryClassLoaderContext ignored =
                     TemporaryClassLoaderContext.of(userClassLoader)) {
            // test connection, fail early if we cannot connect to database
            try (Connection conn = DriverManager.getConnection(this.getJdbcUrl(), username, pwd)) {
            } catch (SQLException e) {
                throw new ValidationException(
                        String.format("Failed connecting to %s via JDBC.", this.getJdbcUrl()), e);
            }
            LOG.info("Catalog {} established connection to {}", getName(), this.getJdbcUrl());
        }
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new SqlServerTableFactory());
    }

    @Override
    protected String getConnector() {
        return SQLSERVER_CONNECTOR;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return extractColumnValuesBySQL(
                this.getJdbcUrl(),
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        return extractColumnValuesBySQL(
                baseUrl + databaseName,
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?",
                1,
                null,
                databaseName);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return !extractColumnValuesBySQL(
                this.getJdbcUrl(),
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                        + "WHERE TABLE_SCHEMA=? and TABLE_NAME=?",
                1,
                null,
                tablePath.getDatabaseName(),
                tablePath.getObjectName())
                .isEmpty();
    }

    private String getDatabaseVersion() {
        try (TemporaryClassLoaderContext ignored =
                     TemporaryClassLoaderContext.of(userClassLoader)) {
            try (Connection conn = DriverManager.getConnection(this.getJdbcUrl(), username, pwd)) {
                return conn.getMetaData().getDatabaseProductVersion();
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed in getting MySQL version by %s.", this.getJdbcUrl()), e);
            }
        }
    }

    private String getDriverVersion() {
        try (TemporaryClassLoaderContext ignored =
                     TemporaryClassLoaderContext.of(userClassLoader)) {
            try (Connection conn = DriverManager.getConnection(this.getJdbcUrl(), username, pwd)) {
                String driverVersion = conn.getMetaData().getDriverVersion();
                Pattern regexp = Pattern.compile("\\d+?\\.\\d+?\\.\\d+");
                Matcher matcher = regexp.matcher(driverVersion);
                return matcher.find() ? matcher.group(0) : null;
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed in getting MySQL driver version by %s.", this.getJdbcUrl()),
                        e);
            }
        }
    }

    /**
     * Converts MySQL type to Flink {@link DataType}.
     */
    @Override
    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        return dialectTypeMapper.mapping(tablePath, metadata, colIndex);
    }

    @Override
    protected String getTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }

    @Override
    protected String getSchemaName(ObjectPath tablePath) {
        return tablePath.getDatabaseName();
    }

    @Override
    protected String getSchemaTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }
}
