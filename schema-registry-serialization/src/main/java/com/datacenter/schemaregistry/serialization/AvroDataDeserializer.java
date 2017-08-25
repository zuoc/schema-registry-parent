package com.datacenter.schemaregistry.serialization;


import com.datacenter.schemaregistry.common.SchemaCompatibilityLevel;
import com.datacenter.schemaregistry.common.SchemaKey;
import com.datacenter.schemaregistry.common.SchemaRegistry;
import com.datacenter.schemaregistry.common.SchemaValue;
import com.datacenter.schemaregistry.common.exceptions.SchemaCompatibilityException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by zuoc on 2017/5/22.
 */
public class AvroDataDeserializer extends AbstractAvroSerialization {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private DecoderFactory decoderFactory = DecoderFactory.get();

    private boolean includeSchema = false;

    private static final ConcurrentMap<String, Class> classCache = new ConcurrentHashMap<>();

    static {
        classCache.put(Schema.Type.BOOLEAN.getName(), Boolean.class);
        classCache.put(Schema.Type.INT.getName(), Integer.class);
        classCache.put(Schema.Type.LONG.getName(), Long.class);
        classCache.put(Schema.Type.FLOAT.getName(), Float.class);
        classCache.put(Schema.Type.DOUBLE.getName(), Double.class);
        classCache.put(Schema.Type.STRING.getName(), String.class);
        classCache.put(Schema.Type.BYTES.getName(), byte[].class);
    }

    public AvroDataDeserializer(SchemaRegistry schemaRegistry, SerializationConfig serializationConfig) {
        super(schemaRegistry, serializationConfig);
    }

    @Override
    protected void init0(SerializationConfig serializationConfig) {

    }

    public Object deserialize(final byte[] data) {
        return deserialize(data, null);
    }

    public Object deserialize(final byte[] data, final Schema readSchema) {
        if (data == null || data.length < SCHEMA_ID_LENGTH + 1) {
            throw new AvroSerializationException("");
        }

        final ByteBuffer buffer = ByteBuffer.wrap(data);

        final Schema writeSchema = getWriteSchema(buffer);

        // 兼容性检查
        if (readSchema != null) {
            final boolean compatible = SchemaCompatibilityLevel.READ.isCompatible(readSchema, writeSchema);
            if (!compatible) {
                throw new SchemaCompatibilityException("");
            }
        }

        if (!buffer.hasRemaining()) {
            throw new AvroSerializationException("");
        }

        if (writeSchema.getType().equals(Schema.Type.BYTES)) {
            final byte[] dist = new byte[buffer.remaining()];
            buffer.get(dist, 0, dist.length);
            return dist;
        }

        try {
            final Class type = getType(readSchema != null ? readSchema : writeSchema);

            final DatumReader datumReader = getDatumReader(writeSchema, readSchema, type);
            final Decoder decoder = decoderFactory.binaryDecoder(buffer.array(), buffer.position(), buffer.remaining(), null);

            final Object result = IndexedRecord.class.isAssignableFrom(type) ? datumReader.read(null, decoder) : datumReader.read(type.newInstance(), decoder);

            if (includeSchema) {
                return new AvroData(result, readSchema != null ? readSchema : writeSchema);
            }
            return result;

        } catch (IOException e) {
            logger.error("", e);
            throw new AvroSerializationException("");
        } catch (InstantiationException e) {
            logger.error("", e);
            throw new AvroSerializationException("");
        } catch (IllegalAccessException e) {
            logger.error("", e);
            throw new AvroSerializationException("");
        }
    }

    private Class getType(final Schema readSchema) {
        final String className = readSchema.getFullName();
        if (classCache.containsKey(className)) {
            return classCache.get(className);
        }

        Class type = null;

        try {
            type = ClassUtils.forName(className);
        } catch (ClassNotFoundException e) {
            if (!readSchema.getType().equals(Schema.Type.RECORD)) {
                throw new AvroSerializationException("");
            }
            type = GenericData.Record.class;
        }

        final Class absentType = classCache.putIfAbsent(className, type);
        return absentType == null ? type : absentType;
    }

    private DatumReader getDatumReader(final Schema writeSchema, final Schema readSchema, final Class type) {
        if (SpecificRecord.class.isAssignableFrom(type)) {
            return readSchema == null ? new SpecificDatumReader(writeSchema) : new SpecificDatumReader(writeSchema, readSchema);
        }

        if (isPrimitiveSchema(type) || GenericRecord.class.isAssignableFrom(type)) {
            return readSchema == null ? new GenericDatumReader(writeSchema) : new GenericDatumReader(writeSchema, readSchema);
        }

        return readSchema == null ? new ReflectDatumReader(writeSchema) : new ReflectDatumReader(writeSchema, readSchema);
    }


    private Schema getWriteSchema(final ByteBuffer buffer) {
        if (buffer.get() != MAGIC) {
            throw new AvroSerializationException("");
        }

        final long schemaId = buffer.getLong();

        final SchemaValue schemaValue = schemaRegistry.get(SchemaKey.valueOf(schemaId));
        if (schemaValue == null) {
            throw new AvroSerializationException("");
        }

        return correctedSchema(schemaValue.getSchema());
    }

}
