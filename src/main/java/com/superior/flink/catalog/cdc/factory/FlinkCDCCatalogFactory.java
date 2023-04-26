package com.superior.flink.catalog.cdc.factory;

import com.superior.flink.catalog.cdc.*;
import com.superior.flink.cdc.catalog.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.SCHEMA_NAME;
import static org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactoryOptions.BASE_URL;
import static org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactoryOptions.DEFAULT_DATABASE;
import static org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactoryOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactoryOptions.USERNAME;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

public class FlinkCDCCatalogFactory implements CatalogFactory {
    @Override
    public String factoryIdentifier() {
        return FlinkCDCCatalogFactoryOptions.IDENTIFIER;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();

        String baseUrl = helper.getOptions().get(BASE_URL);
        if (StringUtils.startsWith(baseUrl, "jdbc:mysql:")) {
            return new MySqlCatalog(
                    context.getClassLoader(),
                    context.getName(),
                    helper.getOptions().get(DEFAULT_DATABASE),
                    helper.getOptions().get(USERNAME),
                    helper.getOptions().get(PASSWORD),
                    baseUrl);
        } else if (StringUtils.startsWith(baseUrl, "jdbc:postgresql:")) {
            return new PostgresCatalog(
                    context.getClassLoader(),
                    context.getName(),
                    helper.getOptions().get(DEFAULT_DATABASE),
                    helper.getOptions().get(USERNAME),
                    helper.getOptions().get(PASSWORD),
                    baseUrl);
        } else if (StringUtils.startsWith(baseUrl, "jdbc:oracle:")) {
            return new OracleCatalog(
                    context.getClassLoader(),
                    context.getName(),
                    helper.getOptions().get(DEFAULT_DATABASE),
                    helper.getOptions().get(USERNAME),
                    helper.getOptions().get(PASSWORD),
                    baseUrl);
        } else if (StringUtils.startsWith(baseUrl, "jdbc:sqlserver:")) {
            return new SqlServerCatalog(
                    context.getClassLoader(),
                    context.getName(),
                    helper.getOptions().get(DEFAULT_DATABASE),
                    helper.getOptions().get(SCHEMA_NAME),
                    helper.getOptions().get(USERNAME),
                    helper.getOptions().get(PASSWORD),
                    baseUrl);
        } else if (StringUtils.startsWith(baseUrl, "jdbc:sqlserver:")) {
            return new Db2Catalog(
                    context.getClassLoader(),
                    context.getName(),
                    helper.getOptions().get(DEFAULT_DATABASE),
                    helper.getOptions().get(USERNAME),
                    helper.getOptions().get(PASSWORD),
                    baseUrl);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Catalog for '%s' is not supported yet.", baseUrl));
        }
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(BASE_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPERTY_VERSION);
        options.add(SCHEMA_NAME);
        return options;
    }
}
