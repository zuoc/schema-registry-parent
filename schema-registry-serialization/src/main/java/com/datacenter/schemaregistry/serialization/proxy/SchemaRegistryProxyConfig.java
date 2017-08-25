package com.datacenter.schemaregistry.serialization.proxy;


import com.datacenter.schemaregistry.common.SchemaRegistryConfig;

/**
 * Created by zuoc on 2017/5/23.
 */
public class SchemaRegistryProxyConfig extends SchemaRegistryConfig {

    private String remoteHost;

    public String getRemoteHost() {
        return remoteHost;
    }

    public void setRemoteHost(String remoteHost) {
        this.remoteHost = remoteHost;
    }
}
