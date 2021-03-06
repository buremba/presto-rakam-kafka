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

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;

import javax.inject.Inject;
import java.util.Objects;

/**
 * Kafka specific implementation of the Presto Connector SPI. This is a read only connector.
 */
public class KafkaConnector
        implements Connector
{
    private final KafkaMetadata metadata;

    private final KafkaSplitManager splitManager;
    private final KafkaPageSourceProvider pageSourceProvider;
    private final KafkaHandleResolver handleResolver;

    @Inject
    public KafkaConnector(
            KafkaHandleResolver handleResolver,
            KafkaMetadata metadata,
            KafkaSplitManager splitManager,
            KafkaPageSourceProvider pageSourceProvider)
    {
        this.handleResolver = Objects.requireNonNull(handleResolver, "handleResolver is null");
        this.metadata = Objects.requireNonNull(metadata, "metadata is null");
        this.splitManager = Objects.requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = Objects.requireNonNull(pageSourceProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return handleResolver;
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorIndexResolver getIndexResolver()
    {
        throw new UnsupportedOperationException();
    }
}
