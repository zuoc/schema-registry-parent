package com.datacenter.schemaregistry.common;

import org.apache.avro.Schema;

import java.io.Serializable;

/**
 * Created by zuoc on 2017/5/22.
 */
public class SchemaValue implements Serializable {

    private static final Schema.Parser parser = new Schema.Parser();

    private Schema schema;

    private String schemaString;

    public static SchemaValue valueOf(Schema schema) {
        final SchemaValue schemaValue = new SchemaValue();
        schemaValue.schema = schema;
        schemaValue.schemaString = schema.toString();
        return schemaValue;
    }

    public static SchemaValue valueOf(String schemaString) {
        final SchemaValue schemaValue = new SchemaValue();
        schemaValue.schemaString = schemaString;
        schemaValue.schema = parser.parse(schemaString);
        return schemaValue;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getSchemaString() {
        return schemaString;
    }

    @Override
    public String toString() {
        return "SchemaValue{" +
                "schema=" + schema +
                ", schemaString='" + schemaString + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaValue that = (SchemaValue) o;

        if (!schema.equals(that.schema)) return false;
        return schemaString.equals(that.schemaString);

    }

    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + schemaString.hashCode();
        return result;
    }
}
