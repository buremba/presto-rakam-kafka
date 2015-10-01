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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.codehaus.jackson.node.TextNode;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.util.AvroUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

/**
 * Kafka specific record set. Returns a cursor for a topic which iterates over a Kafka partition segment.
 */
public class KafkaConnectorPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(KafkaConnectorPageSource.class);

    private static final int KAFKA_READ_BUFFER_SIZE = 1_000_000;

    private final KafkaSplit split;
    private final KafkaSimpleConsumerManager consumerManager;
    private final List<KafkaColumnHandle> columnHandles;
    private final List<Type> types;
    private final Schema actualSchema;
    private final Schema expectedSchema;
    private final PageDatumReader datumReader;
    private final DecoderFactory decoderFactory;
    private final Consumer[] hiddenColumnSupplier;
    private long cursorOffset;
    private final AtomicBoolean reported = new AtomicBoolean();
    PageBuilder pageBuilder;

    private Iterator<MessageAndOffset> messageAndOffsetIterator;

    private BinaryDecoder decoder;
    private long totalBytes;

    KafkaConnectorPageSource(KafkaSplit split, KafkaSimpleConsumerManager consumerManager, List<KafkaColumnHandle> columnHandles, Metastore metastore)
    {
        this.split = checkNotNull(split, "split is null");
        this.consumerManager = checkNotNull(consumerManager, "consumerManager is null");
        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");

        String[] topicName = split.getTopicName().split("_", 2);
        this.actualSchema = AvroUtil.convertAvroSchema(metastore.getCollection(topicName[0], topicName[1]));

        List<Schema.Field> actualSchemaFields = actualSchema.getFields();
        List<Schema.Field> fields = new ArrayList<>();
        for (KafkaColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isHidden()) {
                fields.add(new Schema.Field("_", Schema.create(Schema.Type.BYTES), "", TextNode.valueOf("")));
            }
            else {
                Schema.Field field = actualSchemaFields.stream()
                        .filter(e -> e.name().equals(columnHandle.getName())).findAny().get();
                fields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()));
            }
        }

        this.expectedSchema = Schema.createRecord("collection", null, null, false);
        expectedSchema.setFields(fields);

        this.types = columnHandles.stream().map(KafkaColumnHandle::getType).collect(toList());
        this.cursorOffset = split.getStart();
        this.pageBuilder = new PageBuilder(types);
        decoderFactory = DecoderFactory.get();

        try {
            datumReader = new PageDatumReader(pageBuilder, actualSchema, expectedSchema);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        Consumer[] hiddenColumnSupplier = columnHandles.stream()
                .filter(e -> e.isHidden())
                .map(this::getSupplier).toArray(Consumer[]::new);
        this.hiddenColumnSupplier = hiddenColumnSupplier.length == 0 ? null : hiddenColumnSupplier;
    }

    private boolean isHidden(String name)
    {
        return name.equals("project") || name.equals("collection") || name.equals("_offset");
    }

    private Consumer getSupplier(KafkaColumnHandle handle)
    {
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(handle.getOrdinalPosition());

        switch (handle.getName()) {
            case "project":
                return new SliceSupplier(blockBuilder, split.getTopicName().split("_", 2)[0]);
            case "collection":
                return new SliceSupplier(blockBuilder, split.getTopicName().split("_", 2)[1]);
            case "_offset":
                return new OffsetSupplier(blockBuilder);

            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return reported.get();
    }

    @Override
    public Page getNextPage()
    {
        return advanceNextPosition();
    }

    @Override
    public void close()
            throws IOException
    {
        pageBuilder.reset();
    }

    private void openFetchRequest()
    {
        if (messageAndOffsetIterator == null) {
            log.debug("Fetching %d bytes from offset %d (%d - %d).", KAFKA_READ_BUFFER_SIZE, cursorOffset, split.getStart(), split.getEnd());
            FetchRequest req = new FetchRequestBuilder()
                    .clientId("presto-worker-" + Thread.currentThread().getName())
                    .addFetch(split.getTopicName(), split.getPartitionId(), cursorOffset, KAFKA_READ_BUFFER_SIZE)
                    .build();

            // TODO - this should look at the actual node this is running on and prefer
            // that copy if running locally. - look into NodeInfo

            FetchResponse fetchResponse = null;

            for (HostAddress hostAddress : split.getNodes()) {
                SimpleConsumer consumer = consumerManager.getConsumer(hostAddress);

                try {
                    fetchResponse = RetryDriver.retry().stopOn(InterruptedException.class)
                            .onRetry(() -> consumerManager.refreshConsumer(hostAddress))
                            .run("kafkaFetch", () -> consumer.fetch(req));
                    break;
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
                catch (Exception e) {
                    log.error(e, "Error while fetching data from Kafka");
                }
            }

            if (fetchResponse == null) {
                throw new IllegalStateException("No Kafka nodes available.");
            }

            if (fetchResponse.hasError()) {
                short errorCode = fetchResponse.errorCode(split.getTopicName(), split.getPartitionId());
                if (errorCode == 1) {
                    throw new PrestoException(KafkaErrorCode.KAFKA_SPLIT_ERROR, format("Could not fetch data from Kafka, error code is %s. Probably Kafka deleted the messages of the offset that you requested.'", errorCode));
                }
                log.warn("Fetch response has error: %d", errorCode);
                throw new PrestoException(KafkaErrorCode.KAFKA_SPLIT_ERROR, "Could not fetch data from Kafka, error code is '" + errorCode + "'");
            }

            messageAndOffsetIterator = fetchResponse.messageSet(split.getTopicName(), split.getPartitionId()).iterator();
        }
    }

    public Page advanceNextPosition()
    {
        while (true) {
            if (cursorOffset >= split.getEnd()) {
                endOfData();
                if (pageBuilder.isEmpty()) {
                    return null;
                }
                return pageBuilder.build();
            }
            // Create a fetch request
            openFetchRequest();

            while (messageAndOffsetIterator.hasNext()) {
                MessageAndOffset currentMessageAndOffset = messageAndOffsetIterator.next();

                long messageOffset = currentMessageAndOffset.offset();

                if (messageOffset >= split.getEnd()) {
                    endOfData(); // Past our split end. Bail.
                    return pageBuilder.build();
                }

                if (messageOffset >= cursorOffset) {
                    nextRow(datumReader, currentMessageAndOffset);
                }
            }
            messageAndOffsetIterator = null;
        }
    }

    private boolean endOfData()
    {
        if (!reported.getAndSet(true)) {
            log.debug("Found a total of %d bytes (%d messages expected). Last Offset: %d (%d, %d)",
                    totalBytes, split.getEnd() - split.getStart(),
                    cursorOffset, split.getStart(), split.getEnd());
        }
        return false;
    }

    private boolean nextRow(PageDatumReader datumReader, MessageAndOffset messageAndOffset)
    {
        cursorOffset = messageAndOffset.offset() + 1; // Cursor now points to the next message.
        Message message = messageAndOffset.message();
        totalBytes += message.payloadSize();

        ByteBuffer payload = message.payload();

        byte[] bytes = new byte[payload.remaining()];
        payload.get(bytes);

//        decoder = decoderFactory.directBinaryDecoder(new InputStream() {
//            @Override
//            public int read() throws IOException {
//                return payload.get();
//            }
//        }, decoder);
//

        if (hiddenColumnSupplier != null) {
            for (Consumer consumer : hiddenColumnSupplier) {
                consumer.accept(messageAndOffset);
            }
        }
        decoder = decoderFactory.binaryDecoder(bytes, decoder);

        try {
            datumReader.read(null, decoder);
        }
        catch (Exception e) {
            log.error(e, "couldn't read kafka message");
            return false;
        }

        return true; // Advanced successfully.
    }

    private static class SliceSupplier
            implements Consumer<MessageAndOffset>
    {
        private final Slice slice;
        private final BlockBuilder builder;

        public SliceSupplier(BlockBuilder builder, String str)
        {
            this.slice = Slices.utf8Slice(str);
            this.builder = builder;
        }

        @Override
        public void accept(MessageAndOffset messageAndOffset)
        {
            VARCHAR.writeSlice(builder, slice);
        }
    }

    private static class OffsetSupplier
            implements Consumer<MessageAndOffset>
    {
        private final BlockBuilder builder;

        public OffsetSupplier(BlockBuilder builder)
        {
            this.builder = builder;
        }

        @Override
        public void accept(MessageAndOffset messageAndOffset)
        {
            BIGINT.writeLong(builder, messageAndOffset.offset());
        }
    }
}
