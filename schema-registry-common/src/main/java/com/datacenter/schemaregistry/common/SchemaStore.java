package com.datacenter.schemaregistry.common;

import java.util.List;
import java.util.Set;

/**
 * 模式存储
 * Created by zuoc on 2017/5/22.
 */
public interface SchemaStore {

    void init(SchemaStoreConfig schemaStoreConfig);

    SchemaKey store(String subject, SchemaValue value);

    SchemaValue get(SchemaKey key);

    void remove(SchemaKey key);

    Set<SchemaKey> keys();

    List<SchemaValue> values();

    void close();

}
