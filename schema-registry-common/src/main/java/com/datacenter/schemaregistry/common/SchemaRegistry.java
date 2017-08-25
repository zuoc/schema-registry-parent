package com.datacenter.schemaregistry.common;

import org.apache.avro.Schema;

import java.util.Map;
import java.util.Set;

/**
 * 模式注册表
 * Created by zuoc on 2017/5/22.
 */
public interface SchemaRegistry {

    /**
     * 初始化模式注册表
     *
     * @param schemaRegistryConfig 模式注册表配置
     */
    void init(SchemaRegistryConfig schemaRegistryConfig);

    /**
     * 注册新模式,相同 schema 幂等操作。
     *
     * @param subject 模式主题
     * @param schema  avro 模式
     * @return 注册模式主键
     */
    SchemaKey register(String subject, Schema schema);

    /**
     * 注销具体模式
     *
     * @param schemaKey 模式主键
     */
    void unregister(SchemaKey schemaKey);

    /**
     * 注销模式主题
     *
     * @param subject 模式主题
     * @return 模式主键
     */
    Set<SchemaKey> unregister(String subject);

    /**
     * 获取模式
     *
     * @param schemaKey 模式主键
     * @return 模式值
     */
    SchemaValue get(SchemaKey schemaKey);

    /**
     * 列表模式主题下的所有模式值
     *
     * @param subject 模式主题
     * @return 模式值
     */
    Map<SchemaKey, SchemaValue> list(String subject);

    /**
     * 列表所有注册的模式主题
     *
     * @return 所有模式主题
     */
    Set<String> subjects();

    void close();
}
