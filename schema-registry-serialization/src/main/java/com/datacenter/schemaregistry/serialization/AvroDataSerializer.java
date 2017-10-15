package com.datacenter.schemaregistry.serialization;


import com.datacenter.schemaregistry.common.SchemaKey;
import com.datacenter.schemaregistry.common.SchemaRegistry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zuoc on 2017/5/22.
 */
public class AvroDataSerializer extends AbstractAvroSerialization {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private EncoderFactory encoderFactory = EncoderFactory.get();

    public AvroDataSerializer(SchemaRegistry schemaRegistry, SerializationConfig serializationConfig) {
        super(schemaRegistry, serializationConfig);
    }

    @Override
    protected void init0(SerializationConfig serializationConfig) {

    }

    /**
     * 序列化数据,上游不做兼容性检查。
     * 默认使用模式主题最后一个版本的schema。
     * 若不存在,则使用 Class 反射信息生产 schema。详情见 {@link org.apache.avro.reflect} 包
     *
     * @param subject 数据模式主题
     * @param data    待序列化数据
     */
    public byte[] serialize(final String subject, final Object data) {
        if (subject == null) {
            throw new AvroSerializationException("");
        }
        if (data == null) {
            throw new AvroSerializationException("");
        }
        if (data instanceof AvroData) {
            return serialize(subject, ((AvroData) data).getData(), getWriteSchema(subject, data));
        }
        return serialize(subject, data, getWriteSchema(subject, data));
    }

    public byte[] serialize(final String subject, final Object data, final Schema schema) {
        if (subject == null) {
            throw new AvroSerializationException("");
        }
        if (schema == null) {
            throw new AvroSerializationException("");
        }

        if (data == null) {
            throw new AvroSerializationException("");
        }

        if (data instanceof AvroData) {
            // 该接口使用 AvroData 与 参数 schema 产生歧义
            throw new AvroSerializationException("");
        }

        try {
            final SchemaKey schemaKey = schemaRegistry.register(subject, schema);
            if (schemaKey == null) {
                throw new AvroSerializationException("");
            }

            final ByteArrayOutputStream out = new ByteArrayOutputStream();

            out.write(MAGIC);

            out.write(ByteBuffer.allocate(SCHEMA_ID_LENGTH).putLong(schemaKey.getId().longValue()).array());

            if (schema.getType().equals(Schema.Type.BYTES)) {
                if (!(data instanceof byte[])) {
                    throw new AvroSerializationException("");
                }
                out.write((byte[]) data);

                return out.toByteArray();
            }

            final DatumWriter datumWriter = getDatumWriter(data, schema);
            final Encoder encoder = encoderFactory.binaryEncoder(out, null);

            datumWriter.write(data, encoder);
            encoder.flush();

            return out.toByteArray();

        } catch (IOException error) {
            logger.error("Avro 序列化异常 {}, {}, {}", subject, schema, error);
            throw new AvroSerializationException(error.getMessage());
        }
    }

    private DatumWriter getDatumWriter(final Object data, final Schema schema) {
        if (data instanceof SpecificRecord) {
            return new SpecificDatumWriter(schema);
        }

        if (isPrimitiveSchema(data.getClass()) || data instanceof GenericRecord) {
            return new GenericDatumWriter(schema);
        }

        return new ReflectDatumWriter(schema);
    }

    private Schema getWriteSchema(final String subject, final Object data) {
        if (isPrimitiveSchema(data.getClass())) {
            return getPrimitiveSchema(data.getClass());
        }

        if (data instanceof GenericContainer) {
            return ((GenericContainer) data).getSchema();
        }

        if (data instanceof AvroData) {
            return ((AvroData) data).getSchema();
        }

        /// 这里获取该模式主题下最新版本,当其他生产端产生新的模式,可能存在歧义,先屏蔽
//        final SchemaValue schemaValue = schemaRegistry.get(SchemaKey.valueOf(subject));
//        if (schemaValue != null) {
//            return schemaValue.getSchema();
//        }

        return ReflectData.get().getSchema(data.getClass());
    }

}
