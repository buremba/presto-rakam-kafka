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
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.google.common.collect.ImmutableList;
import org.rakam.collection.event.metastore.Metastore;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final KafkaHandleResolver handleResolver;
    private final KafkaSimpleConsumerManager consumerManager;
    private final Metastore schemaMetastore;

    @Inject
    public KafkaPageSourceProvider(
            KafkaHandleResolver handleResolver,
            KafkaSimpleConsumerManager consumerManager,
            Metastore schemaMetastore)
    {
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        this.consumerManager = checkNotNull(consumerManager, "consumerManager is null");
        this.schemaMetastore = checkNotNull(schemaMetastore, "schemaRegistry is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        KafkaSplit kafkaSplit = handleResolver.convertSplit(split);

        ImmutableList.Builder<KafkaColumnHandle> handleBuilder = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            KafkaColumnHandle columnHandle = handleResolver.convertColumnHandle(handle);
            handleBuilder.add(columnHandle);
        }

        return new KafkaConnectorPageSource(kafkaSplit, consumerManager, handleBuilder.build(), schemaMetastore);
    }
}
