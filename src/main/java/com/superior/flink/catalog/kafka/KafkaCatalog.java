package com.superior.flink.catalog.kafka;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.*;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

public class KafkaCatalog extends BaseKafkaCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCatalog.class);

    private final String bootstrapServers;

    public KafkaCatalog(String name, String bootstrapServers) {
        super(name, "kafka");
        this.bootstrapServers = bootstrapServers;
    }

    public Optional<Factory> getFactory() {
        return Optional.of(new KafkaDynamicTableFactory());
    }

    @Override
    public void open() throws CatalogException {

    }

    @Override
    public void close() throws CatalogException {
        LOG.info("Catalog {} closing", getName());
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return Lists.newArrayList("kafka");
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name must not be blank.");
        if (listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        //@TODO 从kafka 中读取所有topic
        return Lists.newArrayList("quickstart-events");
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        try {
            //@TODO 读取流标 schema信息。
            String[] columnNames = new String[] {"name", "age"};
            DataType[] types = new DataType[] {DataTypes.STRING(), DataTypes.INT()};

            Schema.Builder schemaBuilder = Schema.newBuilder().fromFields(columnNames, types);
            Schema tableSchema = schemaBuilder.build();

            Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR.key(), IDENTIFIER);
            props.put(PROPS_BOOTSTRAP_SERVERS.key(), bootstrapServers);


            return CatalogTable.of(tableSchema, null, org.apache.commons.compress.utils.Lists.newArrayList(), props);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }
}
