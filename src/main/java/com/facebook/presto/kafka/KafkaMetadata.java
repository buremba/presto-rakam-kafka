/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.kafka;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.HyperLogLogType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;

import javax.inject.Inject;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaMetadata
        extends ReadOnlyConnectorMetadata
{
    private static final Logger log = Logger.get(KafkaMetadata.class);

    private final String connectorId;
    private final KafkaConnectorConfig kafkaConnectorConfig;
    private final KafkaHandleResolver handleResolver;
    private final Metastore metastore;

    @Inject
    KafkaMetadata(@Named("connectorId") String connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            Metastore metastore,
            KafkaHandleResolver handleResolver)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.metastore = checkNotNull(metastore, "metastore is null");
        this.kafkaConnectorConfig = checkNotNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        return ImmutableList.copyOf(builder.build());
    }

    @Override
    public KafkaTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return new KafkaTableHandle(connectorId,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        KafkaTableHandle kafkaTableHandle = handleResolver.convertTableHandle(table);
        return getTableMetadata(kafkaTableHandle.toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();

        return builder.build();
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KafkaTableHandle kafkaTableHandle = handleResolver.convertTableHandle(tableHandle);

        ConnectorTableMetadata tableMetadata = getTableMetadata(session, kafkaTableHandle);

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        List<ColumnMetadata> fields = tableMetadata.getColumns();
        if (fields != null) {
            for (int i = 0; i < fields.size(); i++) {
                ColumnMetadata field = fields.get(i);
                KafkaColumnHandle columnHandle = new KafkaColumnHandle(connectorId, i, field.getName(), field.getType(), field.isHidden());
                columnHandles.put(field.getName(), columnHandle);
            }
        }

        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        handleResolver.convertTableHandle(tableHandle);
        KafkaColumnHandle kafkaColumnHandle = handleResolver.convertColumnHandle(columnHandle);

        return kafkaColumnHandle.getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames = prefix.getSchemaName() == null ? listTables(session, null) : ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));

        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        List<SchemaField> schema = metastore.getCollection(schemaTableName.getSchemaName(), schemaTableName.getTableName());

        if (schema == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        builder.add(new ColumnMetadata("_offset", BigintType.BIGINT, false, "kafka offset", true));
        builder.add(new ColumnMetadata("project", VarcharType.VARCHAR, true, "project id", true));
        builder.add(new ColumnMetadata("collection", VarcharType.VARCHAR, true, "project collection id", true));

        for (int i = 0; i < schema.size(); i++) {
            SchemaField field = schema.get(i);
            builder.add(new ColumnMetadata(field.getName().toLowerCase(Locale.ENGLISH), schemaToPrestoType(field.getType()), false, null, false));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }

    private static Type schemaToPrestoType(FieldType schema)
    {
        switch (schema) {
            case STRING:
                return VarcharType.VARCHAR;
            case LONG:
                return BigintType.BIGINT;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case DATE:
                return DateType.DATE;
//            case ARRAY:
//                return new ArrayType(VARCHAR);
            case HYPERLOGLOG:
                return HyperLogLogType.HYPER_LOG_LOG;
            case TIME:
                return TimeType.TIME;
            default:
                throw new PrestoException(KafkaErrorCode.AVRO_TYPE_NOT_SUPPORTED, "Avro type is not compatible with Presto type: " + schema);
        }
    }
}
