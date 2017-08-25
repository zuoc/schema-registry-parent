package com.datacenter.schemaregistry.serialization.kafka;



import com.datacenter.schemaregistry.serialization.AvroDataSerializer;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by zuoc on 2017/5/26.
 */
public class KafkaAvroSerializer implements Serializer {

    private AvroDataSerializer avroDataSerializer;

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return avroDataSerializer.serialize("", data);
    }

    @Override
    public void close() {
        avroDataSerializer.close();
    }
}
