package com.datacenter.schemaregistry.serialization;


import com.datacenter.schemaregistry.common.SchemaRegistry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zuoc on 2017/5/22.
 */
public abstract class AbstractAvroSerialization {

    protected final byte MAGIC = 0xA;

    protected final int SCHEMA_ID_LENGTH = 8;

    protected SchemaRegistry schemaRegistry;

    private static final Map<Type, Schema> primitiveSchemas = new HashMap<>();

    public AbstractAvroSerialization(final SchemaRegistry schemaRegistry, final SerializationConfig serializationConfig) {
        this.schemaRegistry = schemaRegistry;
        init(serializationConfig);
    }

    static {
        primitiveSchemas.put(Boolean.class, Schema.create(Schema.Type.BOOLEAN));
        primitiveSchemas.put(Integer.class, Schema.create(Schema.Type.INT));
        primitiveSchemas.put(Long.class, Schema.create(Schema.Type.LONG));
        primitiveSchemas.put(Float.class, Schema.create(Schema.Type.FLOAT));
        primitiveSchemas.put(Double.class, Schema.create(Schema.Type.DOUBLE));
        primitiveSchemas.put(String.class, Schema.create(Schema.Type.STRING));
        primitiveSchemas.put(byte[].class, Schema.create(Schema.Type.BYTES));
    }

    private void init(final SerializationConfig serializationConfig) {
        init0(serializationConfig);
    }

    protected abstract void init0(final SerializationConfig serializationConfig);

    protected boolean isPrimitiveSchema(final Type type) {
        return primitiveSchemas.containsKey(type);
    }

    protected Schema getPrimitiveSchema(final Type type) {
        return primitiveSchemas.get(type);
    }

    //修正 schema
    protected Schema correctedSchema(final Schema schema) {
        if (schema.getType().equals(Schema.Type.STRING)) {
            schema.addProp(GenericData.STRING_PROP, GenericData.StringType.String);
        }
        return schema;
    }

    public void close() {

    }

}
