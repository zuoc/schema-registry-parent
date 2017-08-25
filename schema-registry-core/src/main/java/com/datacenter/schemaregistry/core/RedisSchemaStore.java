package com.datacenter.schemaregistry.core;



import com.datacenter.schemaregistry.common.SchemaKey;
import com.datacenter.schemaregistry.common.SchemaStore;
import com.datacenter.schemaregistry.common.SchemaStoreConfig;
import com.datacenter.schemaregistry.common.SchemaValue;
import com.datacenter.schemaregistry.common.exceptions.OptimisticLockFailedException;
import com.datacenter.schemaregistry.common.exceptions.SchemaStoreException;
import com.datacenter.schemaregistry.common.exceptions.SchemaValueExistsException;
import com.datacenter.schemaregistry.common.utils.IdGeneratorUtil;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * schemaregistry:schemas:subject:version:id -> "schema"
 * schemaregistry:versions:subject -> "version"
 * schemaregistry:md5:subject -> set("md5")
 *
 * Created by zuoc on 2017/5/22.
 */
public class RedisSchemaStore implements SchemaStore {

    private RedisTransactionTemplate redisTransactionTemplate;

    private RedisTemplate<String, String> redisTemplate;

    private final String SUBJECT = "%subject%";
    private final String VERSION = "%version%";
    private final String ID = "%id%";

    private final String SCHEMAS_PREFIX = "schemaregistry:schemas";
    private final String SCHEMAS_PATTERN = SCHEMAS_PREFIX + ":" + SUBJECT + ":" + VERSION + ":" + ID;

    private final String VERSIONS_PREFIX = "schemaregistry:versions";
    private final String VERSIONS_PATTERN = VERSIONS_PREFIX + ":" + SUBJECT;

    private final String MD5_PREFIX = "schemaregistry:md5";
    private final String MD5_PATTERN = MD5_PREFIX + ":" + SUBJECT;

    public RedisSchemaStore(RedisTemplate redisTemplate, SchemaStoreConfig schemaStoreConfig) {
        this.redisTemplate = redisTemplate;
        redisTransactionTemplate = new RedisTransactionTemplate(redisTemplate);
        init(schemaStoreConfig);
    }

    @Override
    public void init(SchemaStoreConfig schemaStoreConfig) {
    }

    @Override
    public SchemaKey store(String subject, SchemaValue value) {
        if (value == null) {
            throw new SchemaStoreException("");
        }
        final String currentVersion = redisTemplate.opsForValue().get(getVersionsKey(subject));
        if (redisTemplate.opsForSet().isMember(getMd5Key(subject), DigestUtils.md5Hex(value.getSchemaString()))) {
            throw new SchemaValueExistsException("模式已经存在!");
        }
        return redisTransactionTemplate.execute(subject, operations -> {
            operations.opsForSet().add(getMd5Key(subject), DigestUtils.md5Hex(value.getSchemaString()));
            operations.opsForValue().increment(getVersionsKey(subject), 1L);
            final Long version = currentVersion == null ? 1L : Long.valueOf(currentVersion) + 1L;
            final Long id = IdGeneratorUtil.generatId();
            operations.opsForValue().set(getSchemasKey(subject, version, id), value.getSchemaString());
            return SchemaKey.valueOf(subject, version, id);
        });
    }

    @Override
    public void remove(SchemaKey key) {
        final String delKeyString = getSchemaKey(key);
        if (StringUtils.isBlank(delKeyString)) {
            throw new SchemaStoreException("");
        }

        final SchemaKey delKey = SchemaKey.parse(delKeyString);
        final SchemaValue delValue = get(delKey);
        if (delValue == null) {
            throw new SchemaStoreException("");
        }

        final String md5Key = getMd5Key(delKey.getSubject());
        final String md5Value = DigestUtils.md5Hex(delValue.getSchemaString());
        if (!redisTemplate.opsForSet().isMember(md5Key, md5Value)) {
            throw new SchemaStoreException("");
        }

        redisTransactionTemplate.execute(delKey.getSubject(), operations -> {
            operations.opsForSet().remove(md5Key, md5Value);
            operations.delete(delKeyString);
            return null;
        });
    }

    @Override
    public SchemaValue get(SchemaKey key) {
        final String schemaKey = getSchemaKey(key);
        if (schemaKey != null) {
            return SchemaValue.valueOf(redisTemplate.opsForValue().get(schemaKey));
        }
        return null;
    }

    @Override
    public Set<SchemaKey> keys() {
        return redisTemplate.keys(getMatchSchemaKey()).parallelStream().map(SchemaKey::parse).collect(Collectors.toSet());
    }

    @Override
    public List<SchemaValue> values() {
        return redisTemplate.opsForValue().multiGet(redisTemplate.keys(getMatchSchemaKey())).parallelStream().map(SchemaValue::valueOf).collect(Collectors.toList());
    }

    @Override
    public void close() {

    }

    private String getVersionsKey(final String subject) {
        // schemaregistry:versions:subject
        return VERSIONS_PATTERN.replace(SUBJECT, subject);
    }

    private String getMatchSchemaKey() {
        // schemaregistry:schemas*
        return SCHEMAS_PREFIX + "*";
    }

    private String getSchemaKey(final SchemaKey key) {
        if (key == null) {
            return null;
        }
        if (key.getId() != null) {
            return getSchemaKey(key.getId());
        }

        if (key.getSubject() != null && key.getVersion() != null) {
            return getSchemaKey(key.getSubject(), key.getVersion());
        }

        // FIXME 暂时不实现获取最新版本
        if (key.getSubject() != null) {
            return null;
        }

        return null;
    }

    private String getSchemaKey(final Long id) {
        // schemaregistry:schemas*id
        return (String) redisTemplate.keys(SCHEMAS_PREFIX + "*" + id.toString()).iterator().next();
    }

    private String getSchemaKey(final String subject, final Long version) {
        // schemaregistry:schemas:subject:version:*
        return (String) redisTemplate.keys(SCHEMAS_PATTERN.replace(SUBJECT, subject).replace(VERSION, version.toString()).replace(ID, "*")).iterator().next();
    }

    private String getSchemasKey(final String subject, Long version, Long id) {
        // schemaregistry:schemas:subject:version:id
        return SCHEMAS_PATTERN.replace(SUBJECT, subject).replace(VERSION, version.toString()).replace(ID, id.toString());
    }

    private String getMd5Key(final String subject) {
        // schemaregistry:md5:subject
        return MD5_PATTERN.replace(SUBJECT, subject);
    }

    // redis 事务操作模板
    private class RedisTransactionTemplate {

        private RedisTemplate<String, String> redisTemplate;

        public RedisTransactionTemplate(RedisTemplate redisTemplate) {
            this.redisTemplate = redisTemplate;
        }

        public <T> T execute(String subject, RedisTransactionInvocation invocation) {
            return redisTemplate.execute(new SessionCallback<T>() {
                @Override
                public T execute(RedisOperations operations) throws DataAccessException {
                    // lock free, cas
                    operations.watch(getMd5Key(subject));
                    operations.watch(getVersionsKey(subject));
                    operations.multi();
                    try {
                        final T result = (T) invocation.invoke(operations);
                        final List<?> transactionStatus = operations.exec();
                        if (transactionStatus == null || transactionStatus.isEmpty()) {
                            throw new OptimisticLockFailedException("");
                        }
                        return result;
                    } catch (Exception error) {
                        if (!(error instanceof OptimisticLockFailedException)) {
                            throw new SchemaStoreException(error.getMessage());
                        }
                        throw error;
                    }
                }
            });
        }
    }

    private interface RedisTransactionInvocation {
        Object invoke(RedisOperations operations);
    }

}
