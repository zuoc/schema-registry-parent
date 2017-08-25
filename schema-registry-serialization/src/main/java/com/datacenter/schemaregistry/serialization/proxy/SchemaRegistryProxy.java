package com.datacenter.schemaregistry.serialization.proxy;


import com.datacenter.schemaregistry.common.SchemaKey;
import com.datacenter.schemaregistry.common.SchemaRegistry;
import com.datacenter.schemaregistry.common.SchemaRegistryConfig;
import com.datacenter.schemaregistry.common.SchemaValue;
import com.datacenter.schemaregistry.common.exceptions.SchemaRegistryException;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.Set;

/**
 * Created by zuoc on 2017/5/23.
 */
public class SchemaRegistryProxy implements SchemaRegistry {

    private RestTemplate restTemplate;


    @Override
    public void init(SchemaRegistryConfig schemaRegistryConfig) {
        if (!(schemaRegistryConfig instanceof SchemaRegistryProxyConfig)) {
            throw new SchemaRegistryException("");
        }

        final SchemaRegistryProxyConfig proxyConfig = (SchemaRegistryProxyConfig) schemaRegistryConfig;
        if (StringUtils.isBlank(proxyConfig.getRemoteHost())) {
            throw new SchemaRegistryException("");
        }
    }

    @Override
    public SchemaKey register(String subject, Schema schema) {
        return null;
    }

    @Override
    public void unregister(SchemaKey schemaKey) {

    }

    @Override
    public Set<SchemaKey> unregister(String subject) {
        return null;
    }

    @Override
    public SchemaValue get(SchemaKey schemaKey) {
        return null;
    }

    @Override
    public Map<SchemaKey, SchemaValue> list(String subject) {
        return null;
    }

    @Override
    public Set<String> subjects() {
        return null;
    }

    @Override
    public void close() {

    }
}
