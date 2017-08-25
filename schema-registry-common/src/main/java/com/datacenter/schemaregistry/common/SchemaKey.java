package com.datacenter.schemaregistry.common;

import java.io.Serializable;

/**
 * Created by zuoc on 2017/5/22.
 */
public class SchemaKey implements Serializable {

    private String subject;

    private Long id;

    private Long version;

    private static final int SUBJECT = 2;
    private static final int VERSION = 3;
    private static final int ID = 4;

    /**
     * 指定 schema id
     *
     * @param id schema id
     */
    public static SchemaKey valueOf(Long id) {
        final SchemaKey schemaKey = new SchemaKey();
        schemaKey.id = id;
        return schemaKey;
    }

    /**
     * 指定主题,最新版本
     *
     * @param subject 模式主题
     */
    public static SchemaKey valueOf(String subject) {
        final SchemaKey schemaKey = new SchemaKey();
        schemaKey.subject = subject;
        return schemaKey;
    }

    /**
     * 指定主题和版本
     *
     * @param subject 主题
     * @param version 版本
     */
    public static SchemaKey valueOf(String subject, Long version) {
        final SchemaKey schemaKey = new SchemaKey();
        schemaKey.subject = subject;
        schemaKey.version = version;
        return schemaKey;
    }

    public static SchemaKey valueOf(String subject, Long version, Long id) {
        final SchemaKey schemaKey = new SchemaKey();
        schemaKey.subject = subject;
        schemaKey.version = version;
        schemaKey.id = id;
        return schemaKey;
    }

    public static SchemaKey parse(final String schemaKeyString) {
        final String[] elements = schemaKeyString.split(":");
        return valueOf(elements[SUBJECT], Long.valueOf(elements[VERSION]), Long.valueOf(elements[ID]));
    }

    public String getSubject() {
        return subject;
    }

    public Long getId() {
        return id;
    }

    public Long getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaKey schemaKey = (SchemaKey) o;

        if (subject != null ? !subject.equals(schemaKey.subject) : schemaKey.subject != null)
            return false;
        if (id != null ? !id.equals(schemaKey.id) : schemaKey.id != null) return false;
        return version != null ? version.equals(schemaKey.version) : schemaKey.version == null;

    }

    @Override
    public int hashCode() {
        int result = subject != null ? subject.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaKey{" +
                "subject='" + subject + '\'' +
                ", id=" + id +
                ", version=" + version +
                '}';
    }


}
