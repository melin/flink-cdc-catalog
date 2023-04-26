package com.superior.flink.catalog.kafka.factory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class KafkaCatalogFactoryOptions {

    public static final String IDENTIFIER = "kafka";
    public static final ConfigOption<String> BOOTSTRAP_SERVERS = ConfigOptions.key("properties.bootstrap.servers").stringType().noDefaultValue();

    private KafkaCatalogFactoryOptions() {
    }
}
