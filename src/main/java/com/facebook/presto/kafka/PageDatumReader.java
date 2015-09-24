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
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.util.WeakIdentityHashMap;

import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/07/15 23:02.
 */
public class PageDatumReader implements DatumReader<Void> {
    private final PageBuilder builder;
    private final ResolvingDecoder resolver;

    private ResolvingDecoder creatorResolver = null;
    private final Thread creator = Thread.currentThread();

    public PageDatumReader(PageBuilder pageBuilder, Schema schema) throws IOException {
        this(pageBuilder, schema, schema);
    }

    public PageDatumReader(PageBuilder pageBuilder, Schema actualSchema, Schema expectedSchema) throws IOException {
        this.builder = pageBuilder;
        resolver = getResolver(actualSchema, expectedSchema);
        checkState(actualSchema.getFields() != null, "Not a record");
        checkState(expectedSchema.getFields() != null, "Not a record");
    }

    private static final ThreadLocal<Map<Schema,Map<Schema,ResolvingDecoder>>>
            RESOLVER_CACHE =
            new ThreadLocal<Map<Schema,Map<Schema,ResolvingDecoder>>>() {
                protected Map<Schema,Map<Schema,ResolvingDecoder>> initialValue() {
                    return new WeakIdentityHashMap<>();
                }
            };

    protected final ResolvingDecoder getResolver(Schema actual, Schema expected)
            throws IOException {
        Thread currThread = Thread.currentThread();
        ResolvingDecoder resolver;
        if (currThread == creator && creatorResolver != null) {
            return creatorResolver;
        }

        Map<Schema, ResolvingDecoder> cache = RESOLVER_CACHE.get().get(actual);
        if (cache == null) {
            cache = new WeakIdentityHashMap<>();
            RESOLVER_CACHE.get().put(actual, cache);
        }
        resolver = cache.get(expected);
        if (resolver == null) {
            resolver = DecoderFactory.get().resolvingDecoder(Schema.applyAliases(actual, expected), expected, null);
            cache.put(expected, resolver);
        }

        if (currThread == creator){
            creatorResolver = resolver;
        }

        return resolver;
    }

    @Override
    public void setSchema(Schema schema) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Void read(Void reuse, Decoder in) throws IOException {
        resolver.configure(in);

        readRecord(resolver);
        builder.declarePosition();
//        resolver.drain();
        return null;
    }

    protected void readRecord(ResolvingDecoder in) throws IOException {
        for (Schema.Field field : in.readFieldOrder()) {
            BlockBuilder blockBuilder = builder.getBlockBuilder(field.pos());
            read(field.schema(), in, blockBuilder);
        }
    }

    protected void read(Schema schema, ResolvingDecoder in, BlockBuilder blockBuilder) throws IOException {
        switch (schema.getType()) {
            case UNION:   read(schema.getTypes().get(in.readIndex()), in, blockBuilder); break;
            case LONG:    blockBuilder.writeLong(in.readLong()).closeEntry(); break;
            case STRING:
                Slice source = Slices.utf8Slice(in.readString());
                blockBuilder.writeBytes(source, 0, source.length()).closeEntry(); break;
            case ENUM:
                blockBuilder.writeByte(in.readEnum()).closeEntry(); break;
            case INT:     blockBuilder.writeInt(in.readInt()).closeEntry(); break;
            case FLOAT:   blockBuilder.writeFloat(in.readFloat()).closeEntry(); break;
            case DOUBLE:  blockBuilder.writeDouble(in.readDouble()).closeEntry(); break;
            case BOOLEAN: blockBuilder.writeByte(in.readBoolean() ? 1 : 0).closeEntry(); break;
            case NULL:
                in.readNull();
                blockBuilder.appendNull(); break;
            case RECORD:
            case FIXED:
            case ARRAY:
            case MAP: throw new UnsupportedOperationException();
            case BYTES: break; // we do not support bytes, this is a hack for hidden presto columns.
            default: throw new AvroRuntimeException("Unknown type: " + schema);
        }
    }

    /** Called to read an array instance.  May be overridden for alternate array
     * representations.*/
    protected Object readArray(Schema expected, ResolvingDecoder in) throws IOException {
        Schema expectedType = expected.getElementType();
        long l = in.readArrayStart();
        long base = 0;
        if (l > 0) {
            Object array = newArray((int) l, expected);
            do {
                for (long i = 0; i < l; i++) {
//                    read(expectedType, in);
//                    addToArray(array, base + i, read(expectedType, in));
                }
                base += l;
            } while ((l = in.arrayNext()) > 0);
            return array;
        } else {
            return newArray(0, expected);
        }
    }

    /** Called to create new array instances.  Subclasses may override to use a
     * different array implementation.  By default, this returns a {@link
     * GenericData.Array}.*/
    @SuppressWarnings("unchecked")
    protected Object newArray(int size, Schema schema) {
        return new GenericData.Array(size, schema);
    }

    /** Skip an instance of a schema. */
    public static void skip(Schema schema, Decoder in) throws IOException {
        switch (schema.getType()) {
            case RECORD:
                for (Schema.Field field : schema.getFields())
                    skip(field.schema(), in);
                break;
            case ENUM:
                in.readInt();
                break;
            case ARRAY:
                Schema elementType = schema.getElementType();
                for (long l = in.skipArray(); l > 0; l = in.skipArray()) {
                    for (long i = 0; i < l; i++) {
                        skip(elementType, in);
                    }
                }
                break;
            case MAP:
                Schema value = schema.getValueType();
                for (long l = in.skipMap(); l > 0; l = in.skipMap()) {
                    for (long i = 0; i < l; i++) {
                        in.skipString();
                        skip(value, in);
                    }
                }
                break;
            case UNION:
                skip(schema.getTypes().get(in.readIndex()), in);
                break;
            case FIXED:
                in.skipFixed(schema.getFixedSize());
                break;
            case STRING:
                in.skipString();
                break;
            case BYTES:
                in.skipBytes();
                break;
            case INT:     in.readInt();           break;
            case LONG:    in.readLong();          break;
            case FLOAT:   in.readFloat();         break;
            case DOUBLE:  in.readDouble();        break;
            case BOOLEAN: in.readBoolean();       break;
            case NULL:                            break;
            default: throw new RuntimeException("Unknown type: "+schema);
        }
    }

}
