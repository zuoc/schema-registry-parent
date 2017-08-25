package com.datacenter.schemaregistry.core;

import com.datacenter.schemaregistry.common.SchemaKey;
import com.datacenter.schemaregistry.common.SchemaRegistry;
import com.datacenter.schemaregistry.common.SchemaRegistryConfig;
import com.datacenter.schemaregistry.common.SchemaStore;
import com.datacenter.schemaregistry.common.SchemaValue;

import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by zuoc on 2017/5/22.
 */
public class SimpleSchemaRegistry implements SchemaRegistry {

    private SchemaStore schemaStore;

    public SimpleSchemaRegistry(SchemaStore schemaStore, SchemaRegistryConfig schemaRegistryConfig) {
        this.schemaStore = schemaStore;
        init(schemaRegistryConfig);
    }

    @Override
    public void init(SchemaRegistryConfig schemaRegistryConfig) {
    }

    @Override
    public SchemaKey register(String subject, Schema schema) {
        final Map<SchemaKey, SchemaValue> map = list(subject);
        final Optional<Map.Entry<SchemaKey, SchemaValue>> exists = map.entrySet().parallelStream()
                .filter(entry -> entry.getValue().getSchema().equals(schema))
                .findFirst();
        if (exists.isPresent()) {
            return exists.get().getKey();
        }
        return schemaStore.store(subject, SchemaValue.valueOf(schema));
    }

    @Override
    public void unregister(SchemaKey schemaKey) {
        schemaStore.remove(schemaKey);
    }

    @Override
    public Set<SchemaKey> unregister(String subject) {
        final Set<SchemaKey> schemaKeys = schemaStore.keys();

        final Set<SchemaKey> unregisterKeys = schemaKeys.parallelStream()
                .filter(schemaKey -> schemaKey.getSubject().equals(subject))
                .collect(Collectors.toSet());
        unregisterKeys.parallelStream().forEach(schemaStore::remove);
        return unregisterKeys;
    }

    @Override
    public SchemaValue get(SchemaKey schemaKey) {
        return schemaStore.get(schemaKey);
    }

    @Override
    public Map<SchemaKey, SchemaValue> list(String subject) {
        final Map<SchemaKey, SchemaValue> map = new HashMap<>();
        final Set<SchemaKey> schemaKeys = schemaStore.keys();
        schemaKeys.parallelStream()
                .filter(schemaKey -> schemaKey.getSubject().equals(subject))
                .forEach(schemaKey -> map.put(schemaKey, schemaStore.get(schemaKey)));
        return map;
    }

    @Override
    public Set<String> subjects() {
        final Set<SchemaKey> schemaKeys = schemaStore.keys();
        final Set<String> subjects = schemaKeys.parallelStream()
                .map(SchemaKey::getSubject)
                .distinct()
                .collect(Collectors.toSet());
        return subjects;
    }

    @Override
    public void close() {
        schemaStore.close();
    }
}
