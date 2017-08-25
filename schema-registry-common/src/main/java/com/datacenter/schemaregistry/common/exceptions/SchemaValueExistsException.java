package com.datacenter.schemaregistry.common.exceptions;

/**
 * Created by zuoc on 2017/5/26.
 */
public class SchemaValueExistsException extends SchemaStoreException {
    public SchemaValueExistsException(String message) {
        super(message);
    }
}
