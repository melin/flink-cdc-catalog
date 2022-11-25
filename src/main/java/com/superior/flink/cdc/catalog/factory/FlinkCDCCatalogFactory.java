package com.superior.flink.cdc.catalog.factory;

import com.superior.flink.cdc.catalog.MySqlCatalog;
import com.superior.flink.cdc.catalog.PostgresCatalog;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

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
        return options;
    }
}
