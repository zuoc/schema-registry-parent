package com.datacenter.schemaregistry.common;

import org.apache.avro.Schema;

import java.util.List;

/**
 * 兼容级别
 * Created by zuoc on 2017/5/22.
 */
public enum SchemaCompatibilityLevel {

    NONE("NONE", SchemaCompatibilityChecker.NO_OP_CHECKER),

    READ("READ", SchemaCompatibilityChecker.READ_CHECKER),

    READ_TRANSITIVE("READ_TRANSITIVE", SchemaCompatibilityChecker.READ_TRANSITIVE_CHECKER),

    WRITE("WRITE", SchemaCompatibilityChecker.WRITE_CHECKER),

    WRITE_TRANSITIVE("WRITE_TRANSITIVE", SchemaCompatibilityChecker.WRITE_TRANSITIVE_CHECKER),

    FULL("FULL", SchemaCompatibilityChecker.FULL_CHECKER),

    FULL_TRANSITIVE("FULL_TRANSITIVE", SchemaCompatibilityChecker.FULL_TRANSITIVE_CHECKER);

    private final String name;

    private final SchemaCompatibilityChecker schemaCompatibilityChecker;

    SchemaCompatibilityLevel(String name, SchemaCompatibilityChecker schemaCompatibilityChecker) {
        this.name = name;
        this.schemaCompatibilityChecker = schemaCompatibilityChecker;
    }

    public boolean isCompatible(Schema newSchema, Schema latestSchema) {
        return this.schemaCompatibilityChecker.isCompatible(newSchema, latestSchema);
    }

    public boolean isCompatible(Schema newSchema, List<Schema> previousSchemas) {
        return this.schemaCompatibilityChecker.isCompatible(newSchema, previousSchemas);
    }
}
