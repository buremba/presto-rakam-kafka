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
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.primitives.Ints;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Kafka specific connector column handle.
 */
public final class KafkaColumnHandle
        implements ColumnHandle, Comparable<KafkaColumnHandle>
{
    private final String connectorId;
    private final int ordinalPosition;

    /**
     * Column Name
     */
    private final String name;

    /**
     * Column type
     */
    private final Type type;

    /**
     * True if the column should be hidden.
     */
    private final boolean hidden;

    @JsonCreator
    public KafkaColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("hidden") boolean hidden)

    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.ordinalPosition = ordinalPosition;
        this.name = checkNotNull(name, "name is null");
        this.type = checkNotNull(type, "type is null");
        this.hidden = hidden;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(name, type, false, null, hidden);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(connectorId, ordinalPosition, name, type, hidden);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        KafkaColumnHandle other = (KafkaColumnHandle) obj;
        return Objects.equal(this.connectorId, other.connectorId) &&
                Objects.equal(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equal(this.name, other.name) &&
                Objects.equal(this.type, other.type) &&
                Objects.equal(this.hidden, other.hidden);
    }

    @Override
    public int compareTo(KafkaColumnHandle otherHandle)
    {
        return Ints.compare(this.getOrdinalPosition(), otherHandle.getOrdinalPosition());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("ordinalPosition", ordinalPosition)
                .add("name", name)
                .add("type", type)
                .add("hidden", hidden)
                .toString();
    }
}
