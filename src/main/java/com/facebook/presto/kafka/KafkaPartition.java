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
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Kafka specific partition representation. Each partition maps to a topic partition and is split along segment boundaries.
 */
public class KafkaPartition
        implements ConnectorPartition
{
    private final String topicName;
    private final int partitionId;
    private final HostAddress partitionLeader;
    private final List<HostAddress> partitionNodes;
    private final TupleDomain tupleDomain;
    private final List<Range> offsets;

    public KafkaPartition(String topicName,
            int partitionId,
            HostAddress partitionLeader,
            List<HostAddress> partitionNodes,
            TupleDomain tupleDomain,
            List<Range> offsets)
    {
        this.topicName = Objects.requireNonNull(topicName, "schema name is null");
        this.partitionId = partitionId;
        this.partitionLeader = Objects.requireNonNull(partitionLeader, "partitionLeader is null");
        this.partitionNodes = ImmutableList.copyOf(Objects.requireNonNull(partitionNodes, "partitionNodes is null"));
        this.tupleDomain = Objects.requireNonNull(tupleDomain, "domain is null");
        this.offsets = offsets == null ? null : Collections.unmodifiableList(offsets);
    }

    @Nullable
    public List<Range> getOffsets()
    {
        return offsets;
    }

    @Override
    public String getPartitionId()
    {
        return Integer.toString(partitionId);
    }

    @Override
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return TupleDomain.all();
    }

    public String getTopicName()
    {
        return topicName;
    }

    public int getPartitionIdAsInt()
    {
        return partitionId;
    }

    public HostAddress getPartitionLeader()
    {
        return partitionLeader;
    }

    public List<HostAddress> getPartitionNodes()
    {
        return partitionNodes;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("topicName", topicName)
                .add("partitionId", partitionId)
                .add("partitionLeader", partitionLeader)
                .add("partitionNodes", partitionNodes)
                .toString();
    }
}
