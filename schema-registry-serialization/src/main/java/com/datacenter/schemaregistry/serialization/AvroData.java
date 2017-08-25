package com.datacenter.schemaregistry.serialization;

import org.apache.avro.Schema;

/**
 * Created by zuoc on 2017/5/26.
 */
public final class AvroData {

    private final Object data;

    private final Schema schema;

    public AvroData(Object data, Schema schema) {
        this.data = data;
        this.schema = schema;
    }

    public Object getData() {
        return data;
    }

    public Schema getSchema() {
        return schema;
    }
}
