package com.superior.flink.catalog.jdbc.factory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class JdbcCatalogFactoryOptions {
    // oracle sid
    public static final ConfigOption<String> SID =
            ConfigOptions.key("sid").stringType().noDefaultValue();
}
