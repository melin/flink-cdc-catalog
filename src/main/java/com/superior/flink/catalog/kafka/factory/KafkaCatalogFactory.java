package com.superior.flink.catalog.kafka.factory;

import com.superior.flink.catalog.kafka.KafkaCatalog;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static com.superior.flink.catalog.kafka.factory.KafkaCatalogFactoryOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.PROPS_GROUP_ID;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

public class KafkaCatalogFactory implements CatalogFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return KafkaCatalogFactoryOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BOOTSTRAP_SERVERS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();

        return new KafkaCatalog(
                context.getName(),
                helper.getOptions().get(BOOTSTRAP_SERVERS));
    }
}
