package com.datacenter.schemaregistry.common.exceptions;

/**
 * Created by zuoc on 2017/5/25.
 */
public class OptimisticLockFailedException extends SchemaStoreException {

    public OptimisticLockFailedException(String message) {
        super(message);
    }
}
